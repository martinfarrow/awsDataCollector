import boto3
import botocore
from botocore.exceptions import ClientError
import base64
import hashlib
import os.path
import json
import dateutil.parser as dp
import datetime
import re
import logging 
import os
import sys
import fluentWrap as fl

# needed for exception handling on fabric connections

import fabric 
import socket
import paramiko

MAX_NUM_THROTTLE_RETRIES = 16

class awsDataCollector(Exception):
    pass

class awsDataCollector():
    """Collects related information about common aws artefacts"""

    def __init__(self, session, log, build_env=None):
        self.log = log
        my_retry_config = botocore.config.Config(retries={'max_attempts': MAX_NUM_THROTTLE_RETRIES})

        # boto3 connectors, clients and resources
        self.ec2 = session.resource('ec2', config=my_retry_config)
        self.asg = boto3.client('autoscaling', config=my_retry_config)
        self.ec2Client = session.client('ec2', config=my_retry_config)
        self.r53 = boto3.client('route53', config=my_retry_config)
        self.ssm = boto3.client('ssm', config=my_retry_config)
        self.elasticache = boto3.client('elasticache', config=my_retry_config)

        # hosted zone management
        self.dns = dict()

        # resource records
        self.rr = dict()
        self.ipRRCrossRef=dict()

        # ec2 hosts management
        self.ec2hosts = dict()
        self.ec2hostsById = dict()
        self.instanceCount = dict()

        # ec2 rr cross referencing
        self.ipHostPriv = dict()
        self.ipHostPub = dict()
        self.hostVpc = dict()

        # configuration for fabric to connect to hosts

        self.jump = None
        self.jumpHost = None 
        self.jumpUser = None

        self.jumpPrivKeyFile = None
        self.ec2PrivKeyFile = None

        if (build_env is not None):
            self.setBuildEnv(build_env)

        # asg variables

        self.user_data = False
        self.allAsgs = None
        self.deployAsgs = None

        # ssh writing config

        self.config = dict()
        self.IdentityFile = None
        self.ProxyHost = None
        self.ProxyMatch = None
        self.ec2user = None

        # internal state indicators

        self.configLoaded = False
        self.builtConnections = False

        # load balancing info

        self.elbv2 = boto3.client('elbv2', config=my_retry_config)
        self.elb = boto3.client('elb', config=my_retry_config)
        self.v2lbs = None
        self.lbs = None

        # vpc info

        self.vpc = dict()
        self.subnets = dict()
        self.natgws = dict()

        # ecr info

        self.ecr = boto3.client('ecr')
        self.repos = dict()

        # security groups info
        
        self.sgs = dict()
        self.sgsByGroudId = dict()


    def setBuildEnv(self, build_env):
        self.build_env = build_env
        self.asgRe = re.compile(r'^{}'.format(self.build_env))

    def getLoadBalancers(self):
        response = self.elbv2.describe_load_balancers()
        self.v2lbs = fl.fluentWrap(response['LoadBalancers'])

        response = self.elb.describe_load_balancers()
        self.lbs = fl.fluentWrap(response['LoadBalancerDescriptions'])

    def getRedisReplGroups(self):
        response = self.elasticache.describe_replication_groups()
        self.redis = fl.fluentWrap(response['ReplicationGroups'])

    def getHostedZones(self):
        response = self.r53.list_hosted_zones()
        self.dns = fl.fluentWrap(response['HostedZones'])

    def getAMIData(self):
        # centos account 125523088429
        # amazonlinux-2-base_1579686771 2020-01-22T10:00:28.000Z ami-00b8d754e256a1884 766535289950
        #images = self.ec2Client.describe_images(Owners=['self'])
        fltr = [{'Name': 'name', 'Values': [ 'ami-*' ]}]
        #images = self.ec2Client.describe_images(Filters=fltr)
        images = self.ec2Client.describe_images()
        flimages = fl.fluentWrap(images['Images'])
        for lami in flimages:
            print("{} {} {} {} - {}".format(lami.Name,
                                         lami.CreationDate,
                                         lami.ImageId,
                                         lami.OwnerId,
                                         lami.Description))

    def collectAsgData(self):
        """Collect information about auto-scaling-groups"""

        paginator = self.asg.get_paginator('describe_auto_scaling_groups')
        page_iterator = paginator.paginate()
        allAsgs = page_iterator.build_full_result()['AutoScalingGroups']

        self.allAsgs = fl.fluentWrap(allAsgs)
        self.deployAsgs=dict()
        for asg in self.allAsgs:
            asg.name = self.getNameTag(asg.Tags)
            if self.asgRe.match(asg.name):
                asg.LaunchConfiguration = self.getLaunchConfig(asg)
                self.deployAsgs[asg.name] = asg
        self.deployAsgs = fl.fluentWrap(self.deployAsgs)

    def getAsgNames(self):
        return list(map(lambda x: x.name, self.deployAsgs))

    def getAsgByName(self, name):
        asg = [asg for asg in self.deployAsgs if asg.name == name]
        return asg[0] if len(asg) > 0 else None

    def getLaunchConfig(self, asg):
        response = self.asg.describe_launch_configurations(
                    LaunchConfigurationNames=[asg.LaunchConfigurationName])
        if 'LaunchConfigurations' in response:
            return fl.fluentWrap(response['LaunchConfigurations'])
        return None

    def collectInstanceData(self, collectRdns=False):
        """Collect instance data for the configured filter, and create the e2hosts attribute"""

        try:
            instances = self.ec2.instances.filter(Filters=[{'Name': 'tag:Name', 'Values': [ '{}*'.format(self.build_env)]}])
        except Exception as e:
            print(f"Error", e)
            raise

        self.instanceCount = dict()
        id2name = dict()
        for instance in instances:
            tagHostName = self.getNameTag(instance.tags)
            running = (instance.state['Code'] & 255) <= 32
            if running:
                if tagHostName not in self.instanceCount:
                    self.instanceCount[tagHostName] = -1
                self.instanceCount[tagHostName] = self.instanceCount[tagHostName] + 1

            upSeconds = self.upTime(instance.launch_time)
            id2name[instance.id]={ "name": tagHostName, "instance": instance, 'running': running, 'uptime': upSeconds, 'connection': None}

        # remove zeros, so we don't get name-0 extensions

        for tagHostName in [item for item in self.instanceCount if self.instanceCount[item] == 0]:
            self.instanceCount.pop(tagHostName)

        self.ec2hosts = fl.fluentWrap(id2name)

        # create a shortcut lookup by instance id

        for host in self.ec2hosts:
            self.ec2hostsById[host.instance.id] = host

        self.__setUniqueHostNames()

        if collectRdns:
            self.collectRdnsHostData()

        return
    
    def upTime(self, launch_time):
        """ Gets the time between now and launch time in seconds"""
        lt_datetime = dp.parse(str(launch_time))
        lt_delta = datetime.datetime.now(lt_datetime.tzinfo) - lt_datetime
        return int(lt_delta.total_seconds())

    def getSecurityGroups(self):
        response = self.ec2Client.describe_security_groups(Filters=[{'Name': 'tag:Name', 'Values': [ '{}*'.format(self.build_env) ]}])
        self.sgs = fl.fluentWrap(response['SecurityGroups'])
        self.sgsByGroupId = dict()
        for sg in self.sgs:
            self.sgsByGroupId[sg.GroupId] = sg


    def writeInternalConfig(self, filepath="internal.json"):
        """ experimental to see if I can get this carry state between invocations """
        joutput = dict()
        joutput['Hosts']=self.ec2hosts
        joutput['running']=self.running
        joutput['notRunning']=self.notRunning
        with open(filepath, "w") as fp:
            json.dump(joutput, fp)

    def getUserDataData(self, rawUd):
        ud = base64.b64decode(rawUd)
        md5 = hashlib.md5(str(ud).encode('utf-8')).hexdigest()
        return fl.fluentWrap({"content": ud, "md5": md5})


    def collectRdnsHostData(self):
        """Collect dns cross referencing information"""

        self.hostVpc = dict()
        for host in self.getHosts(running=True):
            self.hostVpc[host.instance.vpc_id] = 1
            if host.instance.private_ip_address is not None:
                self.ipHostPriv[host.instance.private_ip_address] = host
            if host.instance.public_ip_address is not None:
                self.ipHostPub[host.instance.public_ip_address] = host

        # collect rDNS info for cross referencing

        for vpcid in self.hostVpc:
            hzs = self.getHostedZonesByVpc(vpcid)
            for hz in hzs:
                self.getRecordSets(hz.HostedZoneId)

        # get public zones
        self.getHostedZones()
        for hz in self.dns:
            if hz.Config.PrivateZone is False:
                self.getRecordSets(hz.Id)


    def getHosts(self, running):
        """extracts hosts from ec2hosts, running or not-running"""
        if running:
            runners = list(map( lambda x: x if x.running else None, self.ec2hosts))
            runners = [item for item in runners if item is not None]
            return sorted(runners, key=lambda x: x.uptime, reverse = True)
        else:
            notRunning = list(map( lambda x: x if not x.running else None, self.ec2hosts))
            notRunning = [item for item in notRunning if item is not None]
            return sorted(notRunning, key=lambda x: x.uptime, reverse = False)

    def __setUniqueHostNames(self):
        """Makes sure every running Host has a unique alias"""
        for r in self.getHosts(running=True):
            if r.name in self.instanceCount:
                # 1+ ensures we never have a counter-intuative name-0 construct
                newname = r.name + "-" + str(1 + self.instanceCount[r.name])
                self.instanceCount[r.name] -= 1
                r.name = newname

    def loadConfig(self, filepath="./config.json"):
        """ load the external json config file """
        if self.configLoaded:
            return

        self.configLoaded = True
        try:
            with open(filepath) as fp:
                self.config = json.load(fp)
        except IOError as e:
            errno, strerror = e.args
            self.log.error("Unable to open {}: errno({}): {}",format(errno, strerror))
            return
        try:
            self.parseConfig()
        except awsDataCollector as exc:
            self.log.error(exc)
            sys.exit(1)

    def parseConfig(self):
        """ Parse the config file and build internal data structures """

        if '_global' in self.config:
            gbl = self.config['_global']
        else:
            gbl = dict()

        if '_jump' in self.config:
            self.jump = self.config['_jump']
        else:
            self.jump = None

        for build_env in self.config.keys():

            if re.match('^{}'.format(build_env), self.build_env):
                config=self.config[build_env]
                for feature in [ 'IdentityFile', 'ProxyMatch', 'ProxyHost', "ec2user" ]:
                    if feature in config:
                        value = config[feature]
                    elif feature in gbl:
                        value = gbl[feature]
                    else:
                        value = None

                    self.__dict__[feature] = value

                # print a warning here if there isn't local
                # proxyhost defined

                if (self.IdentityFile is None):
                    self.log.warning("IdentityFile is not set, no IdentityFile line will be written to ssh config")

                if (self.ProxyMatch is None):
                    self.log.warning("ProxyMatch is not set, no ssh configs for non-public IPs can be generated")
                if (self.ec2user is None):
                    self.log.warning("No defintion for ec2user has been found, no user line will be added to ssh config")
                return
                break
        raise awsDataCollector("No definition for defined build_env ({}) was found in config file".format(self.build_env))
        return

    def buildConnections(self, debug=False):
        """ builds fabric connections to the detected hosts, using config data and updates the Hosts dictionary """

        if self.builtConnections:
            return

        self.builtConnections = True

        localProxyHost = None
        entryType = None
        jumpCon = None
        localProxyCon = None

        for host in self.getHosts(running=True):
            if re.match('^.*{}.*$'.format(self.ProxyMatch), host.name, re.IGNORECASE):
                if host.instance.public_ip_address is not None:
                    localProxyHost = host
                    if (debug):
                        self.log.info("Host({}) selected as proxy host".format(localProxyHost.name))
                    break

        if debug and localProxyHost is None:
            self.log.warning("No suitable proxy host found")

        if (self.jump is not None and 'jumphost' in self.jump
                and 'jumpuser' in self.jump and 'IdentityFile' in self.jump):

            jumpCon = fabric.Connection(host=self.jump['jumphost'],
                                        user=self.jump['jumpuser'],
                                        connect_kwargs={ "key_filename": os.path.expanduser(self.jump['IdentityFile']) })
            if debug:
                self.log.info("Host({}) configured as jump host".format(self.jump['jumphost']))

        if localProxyHost is not None:
            localProxyCon = fabric.Connection(host=localProxyHost.instance.public_ip_address,
                                              user = self.ec2user,
                                              connect_kwargs={ "key_filename": os.path.expanduser(self.IdentityFile) },
                                              gateway = jumpCon)

        for host in self.getHosts(running=True):

            # check if the host has a public ip                 

            if host.instance.public_ip_address is not None:
                ip = host.instance.public_ip_address
                entryType = 'public'
            elif host.instance.private_ip_address is not None:
                ip = host.instance.private_ip_address
                entryType = 'private'
            else:
                next

            if entryType == 'private' and localProxyCon is None:
                continue

            if entryType == 'private':
                gateway = localProxyCon
            else:
                gateway = jumpCon

            connection = fabric.Connection(host=ip,
                                           user = self.ec2user,
                                           connect_kwargs = {"key_filename": os.path.expanduser(self.IdentityFile)},
                                           connect_timeout = 2,
                                           gateway = gateway)

            #connection.transport.set_keepalive(240)
            # check that we can open the connection

            try:
                connection.open()
                host.connection = connection
                if (debug):
                    self.log.info("Connection to host({}) - OK".format(host.name))
            except socket.timeout as e:
                self.log.warn("Could not connection to host({}) in 2s".format(host.name))
            except paramiko.ssh_exception.PasswordRequiredException:
                self.log.warn("Invalid credentials or encrypted id file for host({})".format(host.name))

    def getECRrepos(self, reponames=list(), images=False):
        """Gather information about ECR repositories"""
        if len(reponames) == 0:
            response = self.ecr.describe_repositories()
        else:
            response = self.ecr.describe_repositories(repositoryNames=reponames)

        self.repos = fl.fluentWrap(response['repositories'])

        for repo in self.repos:
            if (images):
                rresponse = self.ecr.list_images(repositoryName=repo.repositoryName)
                repo.imageIds = fl.fluentWrap(rresponse['imageIds'])
            else:
                repo.imageIds = fl.fluentWrap([])

    def getRecordSets(self, hostzoneid):
        """Get all the record sets in a hosted zone"""

        response = self.r53.list_resource_record_sets(HostedZoneId=hostzoneid)
        answer = self.__recordSetLoop(response)
        self.rr[hostzoneid] = fl.fluentWrap(response['ResourceRecordSets'])

        while answer.isTruncated:
            response = self.r53.list_resource_record_sets(
                                                HostedZoneId=hostzoneid,
                                                StartRecordName = answer.NextRecordName,
                                                StartRecordType = answer.NextRecordType,
                                                NextRecordIdentifier = answer.NextRcordIdentifier)
            self.rr[hostzoneid] = self.rr[hostzoneid] + fl.fluentWrap(response['ResourceRecordSets'])
            answer = self.__recordSetLoop(response)

        # build a cross reference from the IPaddress to the resource record

        for rr in self.rr[hostzoneid]:
            if rr.Type == 'A':
                for rrs in rr.ResourceRecords:
                    if rr.Value not in self.ipRRCrossRef:
                        self.ipRRCrossRef[rrs.Value] = list()
                    self.ipRRCrossRef[rrs.Value].append(rr)

    def __recordSetLoop(self, response):
        for answer in [ 'isTruncated', 'NextRecordName', 'NextRecordType', 'NextRecordIdentifier']:
            part = dict()
            if answer in response:
                part[answer]=response[answer]

        return fl.fluentWrap(part) 

    def getHostedZonesByVpc(self, vpcId, vpcRegion='eu-west-2'):
        r53response = self.r53.list_hosted_zones_by_vpc(VPCId=vpcId,
                                                        VPCRegion=vpcRegion)
        return(fl.fluentWrap(r53response['HostedZoneSummaries']))

    def getVPCs(self, getSubnets=False, getNATGWs=False, getHostedZones=False):
        """ Gather information about VPCs """
        response = self.ec2Client.describe_vpcs()
        self.vpc = dict() 
        for result in response['Vpcs']:
            vpcId = result['VpcId']
            vpc = self.ec2.Vpc(vpcId)

            name = self.getNameTag(vpc.tags, "unnamed")
            name = self.__getUniqueKey(name, self.vpc)

            if getSubnets or getNATGWs:
                subnets = self.getSubnets(vpc=vpc, getNATGWs=getNATGWs)
            else:
                subnets = list()

            if getHostedZones:
                hz = self.getHostedZonesByVpc(vpcId)
            else:
                hz = list()

            self.vpc[name] = fl.fluentWrap({ 'vpc': vpc,
                          'id': vpcId,
                          'hostedZones': hz,
                          'default': 'True' if vpc.is_default else 'False',
                          'cidr': vpc.cidr_block,
                          'subnets': subnets })

    def getSubnets(self, vpc, getNATGWs=False):
        subnets = dict()
        for subnet in vpc.subnets.all():
            name = self.getNameTag(subnet.tags)
            name = self.__getUniqueKey(name, self.subnets)

            if getNATGWs:
                ngwFilters = [{'Name': 'subnet-id', 'Values': [ subnet.subnet_id ]}]
                natgws = self.getNatGWs(ngwFilters)
            else:
                natgws = list()

            self.subnets[name] = { 'subnet': subnet,
                                   'name': name,
                                   'id': subnet.subnet_id,
                                   'cidr': subnet.cidr_block,
                                   'ngws': natgws }
            subnets[name] = self.subnets[name]

        return subnets

    def getNatGWs(self, ngwFilters = list()):
        natGW = self.ec2Client.describe_nat_gateways(Filters=ngwFilters)
        natGWs = fl.fluentWrap(natGW['NatGateways'])
        natgws = dict()
        for natGW in natGWs:
            name = self.getNameTag(natGW.Tags)
            name = self.__getUniqueKey(name, self.natgws)
            addlist = list()
            for address in natGW.NatGatewayAddresses:
                addlist.append({'public': address.PublicIp,
                                'private': address.PrivateIp,
                                'allocid': address.AllocationId})

            self.natgws[name] = { 'id': natGW.NatGatewayId,
                                  'name': name,
                                  'state': natGW.State,
                                  'addresses' : addlist }

            natgws[name] = self.natgws[name]
        return natgws

    def isAsgSuspended(self, asg):
        """Check if the passed asg is suspended"""
        if isinstance(asg, fl.fluentWrap):
            return asg.SuspendedProcesses.len() > 0
        elif isinstance(asg, str):
            return self.isAsgSuspended(self.getAsgByName(asg))
        else:
            return None


    def __getUniqueKey(self, keyname, indict):
        count=0
        name=keyname
        while name in indict:
            name = keyname + "-" + str(count)
            count+=1
        return name

    def getNameTag(self, listDict, NotSet="NotSet"):
        if listDict is None:
            return NotSet
        for d in listDict:
            if isinstance(d, fl.fluentWrap):
                if d.Key == 'Name':
                    return d.Value
            elif d['Key'] == 'Name':
                return(d['Value'])
        return NotSet
    
    def __del__(self):
        for host in self.getHosts(running=True):
            try:
                host.connection.close()
            except AttributeError:
                pass

