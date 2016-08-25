#!/usr/bin/python

import subprocess
import json
import os
import MySQLdb
import time
import sys
import re
import multiprocessing
import uuid
import argparse


def fetch_data(fun_name, random_string, node):
    
	# Open database connection
	for i in range(0,len(data)):
		data_node = data[i]['node']
		if data_node == node:
			db = MySQLdb.connect(data[i]["solus_db_ip"], data[i]["solus_db_username"], data[i]["solus_db_password"], data[i]["solus_database"])
			# prepare a cursor object using cursor() method
			cursor = db.cursor()
	resource_info_inventory = vm_status(fun_name, cursor)
                    
	#inventory_dict = {1: "None", 2: "None", 3: "None", 4: "None", 5: "None", 6: "None"}
	inventory_dict = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}
        
        db_dict = {}
        
        inventory_dict["memory_mb"] = resource_info_inventory["memory_mb"]
        inventory_dict["num_cpu"] = resource_info_inventory["num_cpu"]
        inventory_dict["os_type"] = resource_info_inventory["template"]
        ip_count = 0
        disk_count = 1
        ip_dict = {1 : "None", 2 : "None", 3 : "None", 4: "None", 5 : "None", 6 : "None", 7 : "None", 8 : "None", "server_name" : "None"}
        ip_dict["server_name"] = resource_info_inventory["hostname"]
	ip_dict_db = {}
	
	inventory_dict[disk_count] = resource_info_inventory["disks"]
	
	for i in resource_info_inventory["net"]:
	    ip_count = ip_count + 1
	    if ip_count <= 8:
		ip_dict[ip_count] = i
	
	
        inventory_dict["ips_count"] = ip_count
        
        
        #Create the id_machine entry on the resource table.
        #OS Linux = 0, Windows = 1
        os = 0
        inventory_dict["os_type"] = inventory_dict["os_type"].lower()
        if "windows" in inventory_dict["os_type"]:
            os = 1
        inventory_dict["os_type"] = os
        #print "Found windows"
        
        
	#panel_db = MySQLdb.connect("localhost", "root", "P@ssw0rd", "database")
	panel_db = MySQLdb.connect("192.168.1.10", "root", "p@$$w0rd", "dev")
	# prepare a cursor object using cursor() method
	panel_cursor = panel_db.cursor()
	
        #check if the machine_id is already on servers table
        find_server = """SELECT * FROM servers WHERE id_machine = '%s' AND node = '%s'"""%(fun_name, node)
        panel_cursor.execute(find_server)
        result = len(panel_cursor.fetchall())
        
        
        if result == 0:
            #Create the id_machine entry on the servers table
            now = time.strftime('%Y-%m-%d %H:%M:%S')
            insert_server_entry = """INSERT INTO servers(active, virtualization, id_machine, os, ip1, ip2, ip3, ip4, ip5, ip6, ip7, ip8, created_at, updated_at, server_name, node)
            VALUES (1, 1, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s' )"""%(fun_name, resource_info_inventory["template"],
             ip_dict[1], ip_dict[2], ip_dict[3], ip_dict[4], ip_dict[5], ip_dict[6], ip_dict[7], ip_dict[8],
             now, now, ip_dict["server_name"], node)
            
            
          
            #insert_resource_entry = """INSERT INTO server_resources(id_server, cpu, ram, ips_count, os_type, disk1, disk2, disk3, disk4, disk5, disk6, created_at, updated_at)
            #VALUES( '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s')"""%(j, inventory_dict["num_cpu"], inventory_dict["memory_mb"], ip_count, os,
             #disk_dict[1], disk_dict[2], disk_dict[3], disk_dict[4], disk_dict[5], disk_dict[6], now, now)
            
            insert_resource_entry = """INSERT INTO server_resources(id_server, cpu, ram, ips_count, os_type, disk1, disk2, disk3, disk4, disk5, disk6, created_at, updated_at, node)
            VALUES( '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s', '%s' )"""%(fun_name, inventory_dict["num_cpu"], inventory_dict["memory_mb"], ip_count, os,
             inventory_dict[1], inventory_dict[2], inventory_dict[3], inventory_dict[4], inventory_dict[5], inventory_dict[6], now, now, node)
            
            #print insert_server_entry
            #print insert_resource_entry
            print "Creating entry on table servers and server_resources for VM with ID %s"%fun_name
            panel_cursor.execute(insert_server_entry)
            panel_cursor.execute(insert_resource_entry)
            panel_db.commit()
        elif result > 0:
            print "servers table already has an entry for VM with ID %s"%fun_name
            #get the latest vm info from the resources table
            #select * from server_resources where id_server = "52a9988d-ed72-7f33-1c2f-8473420a6b07" ORDER BY updated_at DESC;
            latest_vm_info = """ SELECT * FROM server_resources WHERE id_server = '%s' AND node = '%s' ORDER BY updated_at DESC LIMIT 1"""%(fun_name, node)
            panel_cursor.execute(latest_vm_info)
            results = panel_cursor.fetchall()
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
            get_ip_query = """SELECT ip1, ip2,ip3,ip4,ip5,ip6,ip7,ip8, server_name FROM servers where id_machine = '%s' AND node = '%s' """%(fun_name, node)
            panel_cursor.execute(get_ip_query)
            ip_results = panel_cursor.fetchall()
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
                print "Resource haven't changed for the VM with ID %s"%fun_name
            else:
                #resource changed, add an entry to the db with the latest resource info from the inventory
                print "Detected resource change for the VM with ID %s"%fun_name
		print inventory_dict
		print db_dict
                #insert a new entry on resources table with latest info from inventory dict
                now = time.strftime('%Y-%m-%d %H:%M:%S')
                insert_resource_again = """INSERT INTO server_resources(id_server, cpu, ram, ips_count, os_type, disk1, disk2, disk3, disk4, disk5, disk6, created_at, updated_at, node)
                VALUES( '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s', '%s')"""%(fun_name, inventory_dict["num_cpu"], inventory_dict["memory_mb"], ip_count, os,
                inventory_dict[1], inventory_dict[2], inventory_dict[3], inventory_dict[4], inventory_dict[5], inventory_dict[6], now, now, node)
                
                panel_cursor.execute(insert_resource_again)
                panel_db.commit()
                print "Inserted the new resource info for the VM with ID %s"%fun_name
                
            #print ip_dict
            #print ip_dict_db
	    
            compare_ip = cmp(ip_dict, ip_dict_db)
            if compare_ip == 0 :
                print "IP haven't changed yet for VM with ID %s"%fun_name
            else:
                print "Detected change in IP or server name for VM with ID %s"%fun_name
		print ip_dict
		print ip_dict_db
                now = time.strftime('%Y-%m-%d %H:%M:%S')
                update_ip_query = """UPDATE servers SET ip1 = '%s', ip2 = '%s', ip3 = '%s', ip4 = '%s', ip5 = '%s', ip6 = '%s', ip7 = '%s', ip8 = '%s', updated_at = '%s', server_name = '%s' WHERE
                id_machine = '%s' AND node = '%s' """%(ip_dict[1], ip_dict[2], ip_dict[3], ip_dict[4], ip_dict[5], ip_dict[6], ip_dict[7], ip_dict[8], now, ip_dict["server_name"], fun_name, node)
                panel_cursor.execute(update_ip_query)
                panel_db.commit()
                print "Updated the new IP or server name info for the VM with ID %s"%fun_name
                
            
        #print inventory_dict
        #print db_dict
	#Code to set the VM check ID :
	check_id_query = """UPDATE servers SET check_id = '%s' WHERE id_machine = '%s' AND node = '%s' """%(random_string, fun_name, node)
	panel_cursor.execute(check_id_query)
	panel_db.commit()
	print "Updated the check id %s for the VM with ID %s \n"%(random_string, fun_name)
    
	db.close()
	panel_db.commit()
	panel_db.close()
                
            
            


#To check the activeness of VM i.e VM present in DB but not in inventory
def isActive(random_string):
    # Open database connection
    db = MySQLdb.connect("192.168.1.10", "root", "p@$$w0rd", "dev")
    # prepare a cursor object using cursor() method
    cursor = db.cursor()
    now = time.strftime('%Y-%m-%d %H:%M:%S')
    active_query = """UPDATE servers SET active = 0, updated_at = '%s' WHERE check_id != '%s' AND active = 1 AND virtualization = 1"""%(now, random_string)
    cursor.execute(active_query)
    db.commit()
    db.close()
    print "Updated activeness of all VM's"

    
    
def vm_status(fun_vmname, cursor):
    
    #mysql query to fetch the vm info from required tables
    vserver_select_query = """ SELECT * from vservers WHERE vserverid = '%s'"""%fun_vmname
    cursor.execute(vserver_select_query)
    data = cursor.fetchall()
    for row in data:
	hostname = row[7]
	disk = int(row[11])/1073741824
	ram = int(row[15])/1048576
	template = row[18]

    
    vserver_cpu_query = """ SELECT vz_cpus from vzdata WHERE vserverid = '%s'"""%fun_vmname
    cursor.execute(vserver_cpu_query)
    data = cursor.fetchall()
    for row in data:
	cpu = int(row[0])
	
    ip = []
    vserver_ip_query = """ SELECT ipaddress FROM ipaddresses WHERE vserverid = '%s'"""%fun_vmname
    cursor.execute(vserver_ip_query)
    data = cursor.fetchall()
    for row in data:
	ip.append(row[0])
    
    status = {	  "hostname": hostname,
                  "template": template,
                  "num_cpu": cpu,
                  "memory_mb": ram,
                  "disks": disk,
                  "net": ip}
    #print status
    #pprint(status)
    #print json.dumps(status)
    return status
    
    
def getallvmnames(fun_vmlist, random_string, node):
        for vm in fun_vmlist: 
            fetch_data(vm[0], random_string, node)
	print "Completed task of node %s\n"%node


##### To read config.json details details ####
data = json.loads(open("config.json").read())
random_string = str(uuid.uuid4().get_hex().upper()[0:16])

parser = argparse.ArgumentParser(description="To update DB record for a single VM")
parser.add_argument("--id_server", metavar='id_server', help="UUID of the server")
parser.add_argument("--id_node", metavar='id_node', help="Node ID of the VC from config.json")
args = parser.parse_args()
if args.id_server and args.id_node:
    for i in range(0,len(data)):
        node = data[i]['node']
        if node == args.id_node:
		fetch_data(args.id_server, random_string, node)
    sys.exit(1)
        

for i in range(0,len(data)):
	main_db = MySQLdb.connect(data[i]["solus_db_ip"],data[i]["solus_db_username"],data[i]["solus_db_password"],data[i]["solus_database"] )
	node = data[i]["node"]
	print "Processing node %s\n"%node
	main_cursor = main_db.cursor()
	vmlist_query = """SELECT vserverid FROM vservers"""
	main_cursor.execute(vmlist_query)
	vmlist = main_cursor.fetchall()
	main_db.close()
	getallvmnames(vmlist, random_string, node)

print "im out of for loop"
isActive(random_string)