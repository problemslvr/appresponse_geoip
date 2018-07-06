#######################################################################################
##                               Program Initialization                              ##
#######################################################################################

import os
import sys
import time
import urllib
import cStringIO
import datetime
import pygeoip
from datetime import datetime

thedata = cStringIO.StringIO()
thetable = []

gi = pygeoip.GeoIP('C:\OGE\lib\GeoIP.dat', pygeoip.MEMORY_CACHE)

now = datetime.now()
yr = str(now.year)
mo = str(now.month)
dy = str(now.day)
hr = str(now.hour)
mi = str(now.minute)
se = str(now.second)

topmetrics = 100
launch_time = int(round(time.time()))

error_count = 0

#######################################################################################
##                            Field Modifiable Variables                             ##
#######################################################################################

# Set the following variable (username) to a valid administrative account on your ARX
# appliance. Ensure that the value of the username is enclosed in double quotes.

username = "troeh"		                    

# Set the following variable (password) to a valid password associated with the user
# account you specified for the previous username variable.

password = "thor2028"                             
                  
# For the following variable (arx_boxes), enter IP Address or hostname/FQDN for your 
# ARX appliance(s). Separate multiple appliances with commas. Ensure the IP or 
# hostnames of EACH appliances are enclosed with double quotes.
#
# example: arx_boxes = ["arx3.opnet.com"]
# example: arx_boxes = ["arx3.opnet.com","arx1.opnet.com"] 

arx_boxes = ["arx1.opnet.com"]        

# For the following variable (web_apps), enter a listing of the defined web applications
# in ARX that the script will query. Separate multiple applications with commas and ensure
# application names are fully enclosed with double quotes.

bg = ["Internet"]


#######################################################################################
##                               Initialize the logger                               ##
#######################################################################################

logfile = open('C:\OGE\log\geolocation_insight_' + yr + '_' + mo.zfill(2) + '_' + dy + '_' + hr.zfill(2) + '_' + mi.zfill(2) + '_' + se.zfill(2) + '_' + '.log','a')
logline = str(datetime.now()) + '  ***************The script has started***************'
logfile.write(logline)


#######################################################################################
##                  Construct the shell for the Web Services URL                     ##
#######################################################################################

urlpart1 = "https://"
urlpart2 = ":8443/webservice/DataServiceServlet?type=topValues&UserName=" + username + "&Password=" + password + "&numGroups=3&groupType1=BusinessGroup&groupArgument1="
urlpart3 = "&groupType2=MemberIPs&groupType3=IPAddress&metrics=TPIO&start="
urlpart4 = "&end="
urlpart5 = "&topMetric=TPIO&topCount="
urlpart6 = "&showOnlyValidData=true&showGroupPath=false&csv=true"


#######################################################################################
##                              Main Processing Loop                                 ##
#######################################################################################

start_time = int(sys.argv[1])
end_time = int(sys.argv[2])
	
beg_time1 = datetime.fromtimestamp(start_time)
beg_time2 = beg_time1.strftime('%Y-%m-%d %H:%M')
end_time1 = datetime.fromtimestamp(end_time)
end_time2 = end_time1.strftime('%Y-%m-%d %H:%M')

for host in arx_boxes:
	for i in bg:
		fullurl = urlpart1 + host + urlpart2 + i + urlpart3 + str(start_time) + urlpart4 + str(end_time) + urlpart5 + str(topmetrics) + urlpart6
		logline = '\n' + '\n' + str(datetime.now()) + '  INFO: For Business Group' + i + ' on ' + host + ', attempting to retrieve ' + str(topmetrics) + ' IP Addresses'
		logfile.write(logline)
		logline = '\n' + str(datetime.now()) + '  INFO: Beginning at ' + beg_time2 + ' and ending at ' + end_time2 + ' using the following URL:' + '\n' + fullurl + '\n'
		logfile.write(logline)
		try:
			thepage = urllib.urlopen(fullurl)
		except IOError:
			error_count = error_count + 1
			logline = '\n' + str(datetime.now()) + '  ERROR: Could not connect to host ' + host + " for business group " + i
			logfile.write(logline)
			logfile.write(thedata)
			continue
		thedata = thepage.readlines()

		if 'Group,Throughput' in thedata[0]:
			logline = '\n' + str(datetime.now()) + '  INFO: Got succcessful response for ' + i + " on host " + host
			logfile.write(logline)
		else:
			error_count = error_count + 1
			logline = '\n' + str(datetime.now()) + '  ERROR: Request failed for ' + i + " on host " + host
			logfile.write(logline)
			logfile.write(thedata)
			continue
		response_size = len(thedata)
		if response_size < 2:
			logline = '\n' + str(datetime.now()) + '  INFO: No IPs detected for ' + i + " on host " + host + ', starting at ' + beg_time2 + ' and ending at ' + end_time2
			logfile.write(logline)	
		else:
			num_ips = len(thedata) - 1
			logline = '\n' + str(datetime.now()) + '  INFO: Detected ' + str(num_ips) + ' IP Address for ' + i + " on host " + host + ', starting at ' + beg_time2 + ' and ending at ' + end_time2
			logfile.write(logline)								
			
		for line in thedata:
			if "Throughput" not in line:
				newline_v1 = line.split('>')
				newline_v2 = newline_v1[2]
				newline_v3 = newline_v2.split(',')
				ip_addr = newline_v3[0]
				raw_throughput = newline_v3[1]
				new_throughput = "%0.3f" % float(raw_throughput)	
									
				country = gi.country_name_by_addr(ip_addr)
				country = country.replace(',','')
					
				if country == '':
					#logline = '\n' + str(datetime.now()) + '  INFO: Did not get country for IP Address ' + str(ip_addr)
					#logfile.write(logline)	
					country = 'Internal'
					thetable.append([ip_addr,country,new_throughput])
				else:
					#logline = '\n' + str(datetime.now()) + '  INFO: Got country for IP Address ' + str(ip_addr) + " and it is " + country
					#logfile.write(logline)
					thetable.append([ip_addr,country,new_throughput])
			
sorted(thetable, key=lambda item: item[2])

#######################################################################################
##                              Print the Column Header                              ##
#######################################################################################

Titlerow = "ScriptData:TABULARDATA:headings:col1:IP Address:-sdecr 3:-S5:-w204,col2:Country:-sdecr 2:-S1:-w199,col3:Throughput (Inbound and Outbound) [kbits/sec]:-sdecr 1:-ukB/sec:-S2:-jright:-w147"
sys.stdout.write(Titlerow)

#######################################################################################
##                             Print each individual row                             ##
#######################################################################################

row_id = 0
all_the_rows = ""

for row_item in thetable:
	row_output = ";" + "row" + str(row_id) + ":-gv9.0.0\:clientIP " + row_item[0] + "," + row_item[0] + "," + row_item[1] + "," + row_item[2]
	all_the_rows = all_the_rows +  row_output
	row_id = row_id + 1

sys.stdout.write(all_the_rows)

#######################################################################################
##                                Program Finalization                               ##
#######################################################################################

finish_time = int(round(time.time()))
running_seconds = finish_time - launch_time
total_minutes = int(round(running_seconds / 60))
remaining_seconds = running_seconds % 60

logline = '\n' + '\n' + str(datetime.now()) + '  INFO: Total execution time is ' + str(total_minutes) + ' minutes, and ' + str(remaining_seconds) + ' seconds' 
logfile.write(logline)

if error_count > 0:
	logline = '\n' + '\n' + str(datetime.now()) + '  INFO: There were ' + str(err_count) + ' errors encountered while running the script' 
	logfile.write(logline)

logline = '\n' + str(datetime.now()) + '  ***************The script has ended*****************\n'
logfile.write(logline)

sys.stdout.flush()
logfile.close()
