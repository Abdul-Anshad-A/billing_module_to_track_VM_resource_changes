#!/usr/bin/python

import subprocess
import json
import os
import MySQLdb
import time
import sys
import re
from pysphere import *
from connect_class import Connect
from basic_operations import VmBasicOperations
from pysphere import VIServer, VIProperty, MORTypes, VITask
from pysphere.resources import VimService_services as VI
import multiprocessing
import uuid
import ssl
import argparse


def fetch_data(fun_name, fun_uuid, random_string, node, server, vmobj):
    
    # Open database connection
    db = MySQLdb.connect("192.168.1.10","root","p@$$w0rd","dev" )
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    test = []
    
    
    #To get all the registered VM names from the inventory
    #p = subprocess.Popen(["/root/panel/test.py", "--getallvmnames"], stdout=subprocess.PIPE)
    #output, err = p.communicate()
    #data = json.loads(output)
    data = {fun_name : fun_uuid}
    #To get the vminfo from the inventory
    for i, j in data.iteritems():
        #p = subprocess.Popen(["/root/panel/vm_status.py", "--vmname", i], stdout=subprocess.PIPE)
        #output2, err2 = p.communicate()
        #resource_info_inventory = json.loads(output2)
        resource_info_inventory = vm_status(i, server, vmobj)
	retry = 5
	for k in range(0,retry):
	    if resource_info_inventory["net"] == None:
		print "Retrying to fetch ip address - TRY - %s - %s"%(k, resource_info_inventory["name"])
		time.sleep(5)
		resource_info_inventory = vm_status(i, server, vmobj)
		
	if resource_info_inventory["net"] == None:
	    print "Failed to get ip address even after %s tries for vm %s"%(retry, resource_info_inventory["name"])
        
                
        #inventory_dict = {1: "None", 2: "None", 3: "None", 4: "None", 5: "None", 6: "None"}
	inventory_dict = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}
        
        db_dict = {}
        
        inventory_dict["memory_mb"] = resource_info_inventory["memory_mb"]
        inventory_dict["num_cpu"] = resource_info_inventory["num_cpu"]
        inventory_dict["os_type"] = resource_info_inventory["guest_id"]
        ip_count = 0
        disk_count = 1
        ip_dict = {1 : "None", 2 : "None", 3 : "None", 4: "None", 5 : "None", 6 : "None", 7 : "None", 8 : "None", "server_name" : "None"}
        ip_dict["server_name"] = resource_info_inventory["name"]
	ip_dict_db = {}
        #disk_dict = {1 : None, 2 : None, 3 : None, 4: None, 5 : None, 6 : None}

        
        #To retrieve the disk name, size and ip's from JSON
        for key, value in resource_info_inventory.iteritems():
            if key == "disks":
                for disk_key, disk_value in resource_info_inventory[key].iteritems():
                    for disk_key2, disk_value2 in resource_info_inventory[key][disk_key].iteritems():
                        if disk_key2 == "size":
                            #inventory_dict[disk_key] = int(resource_info_inventory[key][disk_key][disk_key2])/1048576
			    if disk_count <= 6:
				inventory_dict[disk_count] = int(int(resource_info_inventory[key][disk_key][disk_key2])/1048576)
                            #disk_dict[disk_count] = int(inventory_dict[disk_key])/1048576
                            #print "disk size in GB %f"%disk_dict[disk_count]
                            disk_count = disk_count + 1
            if key == "net":
                if resource_info_inventory[key] != None:
                    for net_key in resource_info_inventory[key]:
                        #print net_key
                
                        for ips in net_key["ip_addresses"]:
                            if any(c.isalpha() for c in ips) is False:
                                #print ips
                                ip_count = ip_count + 1
				if ip_count <= 8:
				    ip_dict[ip_count] = ips
				
                                #inventory_dict["ip%s"%ip_count] = ips
        inventory_dict["ips_count"] = ip_count
	if ip_count == 0:
	    with open("debug.txt", "a") as myfile:
		myfile.write(json.dumps(resource_info_inventory))
		myfile.write("\n\n\n")

        test.append(inventory_dict["ips_count"])
        
        
        #Create the id_machine entry on the resource table.
        #OS Linux = 0, Windows = 1
        os = 0
        inventory_dict["os_type"] = inventory_dict["os_type"].lower()
        if "windows" in inventory_dict["os_type"]:
            os = 1
        inventory_dict["os_type"] = os
        #print "Found windows"
        
	if ip_count == 0:
	    ip_server_fails_query_1 = """ INSERT INTO server_ipfails(id_server, created_at, node) VALUES ('%s', NOW(), '%s')"""%(j, node)
	    cursor.execute(ip_server_fails_query_1)
	    db.commit()
	    print "Failed to get even 1 ip address, inserted into server_ipfailes and not updated in servers%s"%j        

	
        #check if the machine_id is already on servers table
        find_server = """SELECT * FROM servers WHERE id_machine = '%s' AND node = '%s' """%(j, node)
        cursor.execute(find_server)
	    
        result = len(cursor.fetchall())
        
        
        if result == 0:
            #Create the id_machine entry on the servers table
            now = time.strftime('%Y-%m-%d %H:%M:%S')
            insert_server_entry = """INSERT INTO servers(active, virtualization, id_machine, os, ip1, ip2, ip3, ip4, ip5, ip6, ip7, ip8, created_at, updated_at, server_name, node)
            VALUES (1, 2, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s' )"""%(j, resource_info_inventory["guest_id"],
             ip_dict[1], ip_dict[2], ip_dict[3], ip_dict[4], ip_dict[5], ip_dict[6], ip_dict[7], ip_dict[8],
             now, now, ip_dict["server_name"], node)
	          
            #insert_resource_entry = """INSERT INTO server_resources(id_server, cpu, ram, ips_count, os_type, disk1, disk2, disk3, disk4, disk5, disk6, created_at, updated_at)
            #VALUES( '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s')"""%(j, inventory_dict["num_cpu"], inventory_dict["memory_mb"], ip_count, os,
             #disk_dict[1], disk_dict[2], disk_dict[3], disk_dict[4], disk_dict[5], disk_dict[6], now, now)
            
            insert_resource_entry = """INSERT INTO server_resources(id_server, cpu, ram, ips_count, os_type, disk1, disk2, disk3, disk4, disk5, disk6, created_at, updated_at, node)
            VALUES( '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s', '%s')"""%(j, inventory_dict["num_cpu"], inventory_dict["memory_mb"], ip_count, os,
             inventory_dict[1], inventory_dict[2], inventory_dict[3], inventory_dict[4], inventory_dict[5], inventory_dict[6], now, now, node)
            
            #print insert_server_entry
            #print insert_resource_entry
            print "Creating entry on table servers and server_resources for VM with ID %s"%j
            cursor.execute(insert_server_entry)
            cursor.execute(insert_resource_entry)
            db.commit()
        elif result > 0:
            print "servers table already has an entry for VM with ID %s"%j
	    #update server table with the node_id for the server
	    #node_query = """UPDATE servers SET node = '%s' WHERE id_machine = '%s' """%(node, j)
	    #cursor.execute(node_query)
	    #db.commit()
            #get the latest vm info from the resources table
            #select * from server_resources where id_server = "52a9988d-ed72-7f33-1c2f-8473420a6b07" ORDER BY updated_at DESC;
            latest_vm_info = """ SELECT * FROM server_resources WHERE id_server = '%s' AND node = '%s' ORDER BY updated_at DESC LIMIT 1"""%(j, node)
            cursor.execute(latest_vm_info)
            results = cursor.fetchall()
            for row in results:
                db_dict["num_cpu"] = int(row[2])
                db_dict["memory_mb"] = int(row[3])
                db_dict["ips_count"] = int(row[4])
                db_dict["os_type"] = int(row[5])
                db_dict[1] = int(row[6]) if row[6] != "None" else row[6]
                db_dict[2] = int(row[7]) if row[7] != "None" else row[7]
                db_dict[3] = int(row[8]) if row[8] != "None" else row[8]
                db_dict[4] = int(row[9]) if row[9] != "None" else row[9]
                db_dict[5] = int(row[10]) if row[10] != "None" else row[10]
                db_dict[6] = int(row[11]) if row[11] != "None" else row[11]
                
            #To get the ip's from server table
            get_ip_query = """SELECT ip1, ip2,ip3,ip4,ip5,ip6,ip7,ip8, server_name FROM servers where id_machine = '%s' AND node = '%s' """%(j, node)
            cursor.execute(get_ip_query)
            ip_results = cursor.fetchall()
            for row in ip_results:
                ip_dict_db[1] = row[0]
                ip_dict_db[2] = row[1]
                ip_dict_db[3] = row[2]
                ip_dict_db[4] = row[3]
                ip_dict_db[5] = row[4]
                ip_dict_db[6] = row[5]
                ip_dict_db[7] = row[6]
                ip_dict_db[8] = row[7]
		ip_dict_db["server_name"] = row[8]
                
                
            #compare the inventory and db resource info and track for changes
            #print inventory_dict
            #print db_dict
            compare_resource = cmp(inventory_dict, db_dict)
            if compare_resource == 0:
                #resource haven't changed yet
                print "Resource haven't changed for the VM with ID %s"%j
            else:
                #resource changed, add an entry to the db with the latest resource info from the inventory
                print "Detected resource change for the VM with ID %s"%j
		print inventory_dict
		print db_dict
		
		if ( ip_count == 0 ):
		    del inventory_dict["ips_count"]
		    del db_dict["ips_count"]
		    comp = cmp(inventory_dict, db_dict)
		    if comp == 0:
			print "IP count is zero and other resources are same - Not adding resource entry in db"
		    else:
			print "IP count is zero but other resources are different, inserting it"
			insert_new_compared_resources = """INSERT INTO server_resources(id_server, cpu, ram, ips_count, os_type, disk1, disk2, disk3, disk4, disk5, disk6, created_at, updated_at, node)
		        VALUES( '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s', '%s')"""%(j, inventory_dict["num_cpu"], inventory_dict["memory_mb"], ip_count, os,
		        inventory_dict[1], inventory_dict[2], inventory_dict[3], inventory_dict[4], inventory_dict[5], inventory_dict[6], now, now, node)
			cursor.execute(insert_new_compared_resources)
			db.commit()
		else:	
		    #insert a new entry on resources table with latest info from inventory dict
		    now = time.strftime('%Y-%m-%d %H:%M:%S')
		    insert_resource_again = """INSERT INTO server_resources(id_server, cpu, ram, ips_count, os_type, disk1, disk2, disk3, disk4, disk5, disk6, created_at, updated_at, node)
		    VALUES( '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s', '%s')"""%(j, inventory_dict["num_cpu"], inventory_dict["memory_mb"], ip_count, os,
		    inventory_dict[1], inventory_dict[2], inventory_dict[3], inventory_dict[4], inventory_dict[5], inventory_dict[6], now, now, node)
		    
		    cursor.execute(insert_resource_again)
		    db.commit()
		    print "Inserted the new resource info for the VM with ID %s"%j
                
            #print ip_dict
            #print ip_dict_db
            compare_ip = cmp(ip_dict, ip_dict_db)
            if compare_ip == 0 :
                print "IP haven't changed yet for VM with ID %s \n"%j
            else:
                print "Detected change in IP for VM with ID %s"%j
		print ip_dict
		print ip_dict_db
                now = time.strftime('%Y-%m-%d %H:%M:%S')
		
		if ( (ip_dict_db[1] != "None") and (ip_dict[1] == "None") ):
		    server_ipfails_query = """ INSERT INTO server_ipfails(id_server, created_at, node) VALUES ('%s', NOW(), '%s')"""%(j, node)
		    print "Failed to get ip address, updated in server_ipfails table %s"%j
		else:
		    update_ip_query = """UPDATE servers SET ip1 = '%s', ip2 = '%s', ip3 = '%s', ip4 = '%s', ip5 = '%s', ip6 = '%s', ip7 = '%s', ip8 = '%s', updated_at = '%s', server_name = '%s' WHERE
		    id_machine = '%s' AND node = '%s' """%(ip_dict[1], ip_dict[2], ip_dict[3], ip_dict[4], ip_dict[5], ip_dict[6], ip_dict[7], ip_dict[8], now, ip_dict["server_name"], j, node)
		    cursor.execute(update_ip_query)
		    db.commit()
		    print "Updated the new IP info for the VM with ID %s \n"%j
                
            
        #print inventory_dict
        #print db_dict
	#Code to set the VM check ID :
	check_id_query = """UPDATE servers SET check_id = '%s' WHERE id_machine = '%s' AND node ='%s' """%(random_string, j, node)
	cursor.execute(check_id_query)
	db.commit()
	print "Updated the check id %s for the VM with ID %s"%(random_string, j)
    db.close()
            
                
            
            


#To check the activeness of VM i.e VM present in DB but not in inventory
def isActive(random_string):
    # Open database connection
    db = MySQLdb.connect("192.168.1.10","root","p@$$w0rd","dev" )
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    now = time.strftime('%Y-%m-%d %H:%M:%S')
    active_query = """UPDATE servers SET active = 0, updated_at = '%s' WHERE check_id != '%s' AND active = 1 AND virtualization = 2"""%(now, random_string)
    cursor.execute(active_query)
    db.commit()
    db.close()
    print "Updated activeness of all VM's"

    
    
def vm_status(fun_vmname, server, vmobj):
    
    #VM_PATH = "[datastore1-data] vm-test-name/vm-test-name.vmx"
    VM_NAME = []
    VM_NAME.append(fun_vmname)

    #s = VIServer()
    #s.connect(HOST, USER, PASSWORD)

    result = server._retrieve_properties_traversal(property_names=["name"], obj_type="VirtualMachine")

    #vm_names = [r.PropSet[0].Val for r in result]
    #print vm_names

    #for vmname in vm_names:
    for vmname in VM_NAME:
        vm = server.get_vm_by_name(vmname)

        diskinfo = {}
        vminfo = vm.get_properties(from_cache=False)

        #pprint(vminfo)
        #exit(1)
	#to ignore test VM's which doesnt have key "disks"
	try:
        	for disk in vminfo["disks"]:
	            diskinfo[disk["label"]] = {"path": disk["descriptor"], "size": disk["capacity"]}
	except:
		#print "go the culprit %s"%vmname
		pass

        if 'hostname' in vminfo:
            hostname = vminfo['hostname']
        else:
            hostname = None

        if 'net' in vminfo:
            net = vminfo['net']
        else:
            net = None

        status = {"name": vm.properties.name,
                  "path": vminfo['path'],
                  "guest_id": vminfo['guest_id'],
                  "hostname": hostname,
                  "overallStatus": vm.properties.overallStatus,
                  "VM_type": vm.properties.config.guestFullName,
                  "num_cpu": vminfo['num_cpu'],
                  "memory_mb": vminfo["memory_mb"],
                  "disks": diskinfo,
                  "net": net}

        #pprint(status)
        #print json.dumps(status)
        return status
    
    
def getallvmnames(fun_vmlist, start, end, random_string, node, vc):
        #vmlist = vmobj.getAllVms()
        #print "inside getallvms"
        vmpath = []
        dict = {}
        #print "%d %d"%(start, end)
	p = multiprocessing.current_process()
	print "Initiating task from start %s to end %s - Process ID %s"%(start, end-1, p.pid)
	#sys.exit()
	if hasattr(ssl, '_create_unverified_context'):
	    ssl._create_default_https_context = ssl._create_unverified_context
        conobj = Connect(my_data[vc]['ip'], my_data[vc]['username'], my_data[vc]['password'])
        node = my_data[vc]['node']
        print "Before connection - Process ID %s"%p.pid
        server  = conobj.connectServer()
        print "Got server connection - Process ID %s"%p.pid

        #### To pass the server object to perform basic vm operations ###
        vmobj = VmBasicOperations(server)
        for vm in fun_vmlist[start:end]: 
            (name, uuid) = vmobj.vmNames(vmobj.vmPath(vm))
            dict[name] = uuid
            fetch_data(name, uuid, random_string, node, server, vmobj)
	print "Completed task from start %s to end %s - Process ID %s"%(start, end-1, p.pid)
	print "Closing connection - Process ID %s"%p.pid
	conobj.disconnectServer()


##### To read the VCENTER server details ####
my_data = json.loads(open("config.json").read())
random_string = str(uuid.uuid4().get_hex().upper()[0:16])

parser = argparse.ArgumentParser(description="To update DB record for a single VM")
parser.add_argument("--id_server", metavar='id_server', help="UUID of the server")
parser.add_argument("--id_node", metavar='id_node', help="Node ID of the VC from config.json")
args = parser.parse_args()
if args.id_server and args.id_node:
    for i in range(0,len(my_data)):
        node = my_data[i]['node']
        if node == args.id_node:
		if hasattr(ssl, '_create_unverified_context'):
		    ssl._create_default_https_context = ssl._create_unverified_context
		s = VIServer()
		server = s
		s.connect(my_data[i]['ip'], my_data[i]['username'], my_data[i]['password'])
		vmobj = VmBasicOperations(server)
		properties = ["config.instanceUuid"]
		expected_uuid = args.id_server
		results = s._retrieve_properties_traversal(property_names=properties,obj_type=MORTypes.VirtualMachine)
		mor = None
		for item in results:
		    found = False
		    for p in item.PropSet:
			if p.Val == expected_uuid:
			    found = True
			    mor = item.Obj
		    if found:
			break
		if not mor:
		    print "Object not found"
		else:
		    prop = VIProperty(s, mor)
		    print prop.name
		    print prop.config.instanceUuid
		    fetch_data(prop.name, prop.config.instanceUuid, random_string, node, server, vmobj)
		s.disconnect()
	
    sys.exit(1)

if __name__ == '__main__':
    for vc in range(0,len(my_data)):
        ##### To establish connection with the VCENTER server #####
	if hasattr(ssl, '_create_unverified_context'):
	    ssl._create_default_https_context = ssl._create_unverified_context
        conobj = Connect(my_data[vc]['ip'], my_data[vc]['username'], my_data[vc]['password'])
        node = my_data[vc]['node']
	print "Processing node - %s \n"%node
        print "Before connection - Main"
        server  = conobj.connectServer()
        print "Got server connection - Main"

        #### To pass the server object to perform basic vm operations ###
        vmobj = VmBasicOperations(server)


        #inventory_dict = {"Hard disk 1" : None, "Hard disk 2" : None, "Hard disk 3" : None, "Hard disk 4" : None, "Hard disk 5" : None, "Hard disk 6" : None}
        
        
    
	#print "Inside main"
	#isActive()
	jobs = []
	vmlist = vmobj.getAllVms()
	#print vmlist
	conobj.disconnectServer()
	vm_num = len(vmlist)
	print "I have to process %s machines"%vm_num
	no_of_subprocess = 20
	print "I'm creating %s workers"%no_of_subprocess
	remainder = vm_num % no_of_subprocess
    
	#handle code for vm_num < no of process
	#here
    
	if remainder == 0:
	    #print "inside remainder"
	    index = vm_num/no_of_subprocess
	    p1 = multiprocessing.Process(target=getallvmnames, args=(vmlist, 0, index, random_string, node, vc,))
	    jobs.append(p1)
	    p1.start()
	
	    for i in range(1, no_of_subprocess):
	        p = multiprocessing.Process(target=getallvmnames, args=(vmlist, index*i, index*(i+1), random_string, node, vc,))
	        jobs.append(p)
	        p.start()
        
	else:
	    vm_num = vm_num - remainder
	    index = vm_num/no_of_subprocess
	    p1 = multiprocessing.Process(target=getallvmnames, args=(vmlist, 0, index, random_string, node, vc,))
	    jobs.append(p1)
	    p1.start()
        
	    for i in range(1, no_of_subprocess - 1):
		p = multiprocessing.Process(target=getallvmnames, args=(vmlist, index*i, index*(i+1), random_string, node, vc,))
		jobs.append(p)
		p.start()

	    pn = multiprocessing.Process(target=getallvmnames, args=(vmlist, index*(no_of_subprocess - 1), (index*no_of_subprocess) + remainder,random_string, node, vc,))
	    jobs.append(pn)
	    pn.start()
	
	for process in jobs:
	    process.join()
    
    isActive(random_string)
