#!/usr/bin/python

import subprocess
import os
from connect_class import Connect

class VmBasicOperations:

	def __init__(self, server):
		self.server = server
		#print "Inside VmBasicOperations"
		#print server.get_server_type()

	def getAllVms(self):
		#print self.server.get_server_type()
		self.vmlist = self.server.get_registered_vms()
		#print "List of registered VM details", self.vmlist
		return self.vmlist

	def getAllTemplates(self):
		template_list = self.server.get_registered_vms(advanced_filters={'config.template':True})
		return template_list


	def vmPath(self, vm):
		self.vmpath = self.server.get_vm_by_path(vm)
		#print self.vmpath
		return self.vmpath

	def vmStatus(self, myvmpath):
		#vmpath = self.server.get_vm_by_path(vm)
		self.myvmpath = myvmpath
		status = self.myvmpath.get_status(basic_status=True)
		name = self.myvmpath.get_property('name')
		return (name, status)

	def vmNames(self, myvmpath):
		self.myvmpath = myvmpath
		name = self.myvmpath.get_property('name')
		#print name
		uuid = self.myvmpath.properties.config.instanceUuid
		#print uuid
		return (name, uuid)

	def powerOn(self, myvmpower):
		self.myvmpower = myvmpower
		if self.myvmpower.is_powering_on() == "True":
			#return 0 if the vm is already trying to power on
			return 0
		elif (self.myvmpower.get_status(basic_status=True)) == "POWERED OFF" or  "SUSPENDED":
			self.myvmpower.power_on()
			return 1
		elif self.myvmpower.get_status(basic_status=True) == "POWERED ON":
			return -1

        def powerOff(self, myvmpower):
                self.myvmpower = myvmpower
                if self.myvmpower.is_powering_off() == "True":
                        #return 0 if the vm is already trying to power off
                        return 0
                elif self.myvmpower.get_status(basic_status=True) == "POWERED ON":
                        self.myvmpower.power_off()
                        return 1
                elif self.myvmpower.get_status(basic_status=True) == "POWERED OFF":
                        return -1

        def reset(self, myvmpower):
                self.myvmpower = myvmpower
                if self.myvmpower.is_resetting() == "True":
                        #return 0 if the vm is already trying to power off
                        return 0
                else:
                        self.myvmpower.reset()
                        return 1

        def suspend(self, myvmpower):
                self.myvmpower = myvmpower
                if self.myvmpower.is_suspending() == "True":
                        #return 0 if the vm is already trying to power off
                        return 0
                else:
                        self.myvmpower.suspend()
                        return 1

###### GUEST POWER OPERATIONS

        def guest_shutdown(self, myvmpower):
                self.myvmpower = myvmpower
		self.myvmpower.shutdown_guest()
		return 1


        def guest_reboot(self, myvmpower):
                self.myvmpower = myvmpower
		self.myvmpower.reboot_guest()
		return 1


        def guest_standby(self, myvmpower):
                self.myvmpower = myvmpower
		self.myvmpower.standby_guest()
		return 1




	def vcenterInfo(self):
		servertype = self.server.get_server_type()
		version = self.server.get_api_version()
		datacenter = self.server.get_datacenters()
		cluster = self.server.get_clusters()
		hosts = self.server.get_hosts()
		datastores = self.server.get_datastores()
		resourcepool = self.server.get_resource_pools()
		return (servertype, version, datacenter, cluster, hosts, datastores, resourcepool)


	def clone(self, clonedetails, ip, username, password):
		self.basename = clonedetails[0]
		self.template = clonedetails[1]
		self.resourcepool = clonedetails[2]
		self.count = clonedetails[3]
		self.amount = clonedetails[4]
		self.ip = ip
		self.username = username
		self.password = password
		#output = subprocess.Popen(["/var/www/html/scripts/misc/pysphere-multi-clone.py", "-s", self.ip, "-u", self.username, "-b", self.basename, "-t", self.template, "-r", self.resourcepool, "-c", self.count, "-n", self.amount], stdout=subprocess.PIPE)
		#clone_output, err = output.communicate()
		cmd = "/var/www/html/scripts/misc/pysphere-multi-clone.py -s " + self.ip + " -u " + self.username +  " -b " +  self.basename + " -t " + self.template + " -r " + self.resourcepool + " -c " +  self.count + " -n " + self.amount
		#print cmd
		f = os.popen(cmd)
		now = f.read()
		#return clone_output
		#print now
		now.strip()
		return now



	def migrate(self):
		mig = self.server.get_vm_by_name("DSL-4.4.10")
		stat = mig.migrate()
		print stat
