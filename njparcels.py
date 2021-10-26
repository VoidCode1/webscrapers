import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
from tqdm import tqdm
import gspread
import time
import re
import threading
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
from fake_useragent import UserAgent




scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name("sheetautomation123-32d1c81c6e38.json", scope)

client = gspread.authorize(creds)
main_sheet = client.open("nj parcels property")

worksheets = main_sheet.worksheets()

repeat = 0

def name_county(counties,town):
	if town.split("/")[2].startswith("01"):
		return counties[0]
	elif town.split("/")[2].startswith("02"):
		return counties[1]
	elif town.split("/")[2].startswith("03"):
		return counties[2]
	elif town.split("/")[2].startswith("04"):
		return counties[3]
	elif town.split("/")[2].startswith("05"):
		return counties[4]
	elif town.split("/")[2].startswith("06"):
		return counties[5]
	elif town.split("/")[2].startswith("07"):
		return counties[6]
	elif town.split("/")[2].startswith("08"):
		return counties[7]
	elif town.split("/")[2].startswith("09"):
		return counties[8]
	elif town.split("/")[2].startswith("10"):
		return counties[9]
	elif town.split("/")[2].startswith("11"):
		return counties[10]
	elif town.split("/")[2].startswith("12"):
		return counties[11]
	elif town.split("/")[2].startswith("13"):
		return counties[12]
	elif town.split("/")[2].startswith("14"):
		return counties[13]
	elif town.split("/")[2].startswith("15"):
		return counties[15]
	elif town.split("/")[2].startswith("16"):
		return counties[15]
	elif town.split("/")[2].startswith("17"):
		return counties[16]
	elif town.split("/")[2].startswith("18"):
		return counties[17]
	elif town.split("/")[2].startswith("19"):
		return counties[18]
	elif town.split("/")[2].startswith("20"):
		return counties[19]
	elif town.split("/")[2].startswith("21"):
		return counties[20]
def njparcels(step):
	url = "https://www.njparcels.com"
	ua = UserAgent()
	header = {'User-Agent':str(ua.chrome)}
	page = requests.get(url)
	page = bs(page.text,"lxml")
	towns = [x.find("a") for x in page.find_all("span",{"class":"muniname"})]
	countynames = [x.text for x in page.find_all("h2",{"class":"countyname"})]
	link = towns[step]
	block_page = requests.get(url+link.get("href"))
	file_name = link.text
	worksheets = main_sheet.worksheets()
	worksheets = [x.title for x in worksheets]
	name = name_county(countynames,link.get("href"))
	name_check = f'{name.upper()} {file_name.lower()}'
	# if name_check in worksheets:
	# 	print(f"{name_check} done")
	# 	return
	# else:
	# repeat +=1
	if block_page.status_code == 200:
		# print(f"Fetching {link.text}")
		block = bs(block_page.text,"lxml")
		multiple = [["Block","Lot","Address","Owner","Type"]]
		blocks = [x.find("td").find("a").get("href") for idx,x in enumerate(block.find_all("tr")) if idx != 0]
		for blck in tqdm(blocks):
			table = requests.get(url+blck)
			table_soup = bs(table.text,"lxml")
			check_table = table_soup.find("table")
			body = check_table.find("tbody")
			for row in body.find_all("tr"):
				pattern = re.compile(r"(\w+|\s\w+|\d+|\s\d+)")
				words = pattern.findall(blck.split("/")[-2].lower())
				sent_first = " ".join(words)
				single = [sent_first]
				for idx,td in enumerate(row.find_all("td")):
					pattern = re.compile(r"(\w+|\s\w+|\d+|\s\d+|\d+\s\d+)")
					words = pattern.findall(td.text.lower())
					sent = " ".join(words)
					single.append(sent)
				multiple.append(single)
		data = pd.DataFrame(multiple)
		data = data.fillna(0)
		values = data.values
		done=True
		while done:
			try:
				time.sleep(np.random.randint((25)))
				new_worksheet = main_sheet.add_worksheet(title=f'{name.upper()} {file_name.lower()}', rows=data.shape[0], cols=data.shape[1])
				new_worksheet.update(values.tolist())
				done=False
				repeat -=1
				
			except gspread.exceptions.APIError:
				time.sleep(np.random.randint((25)))
				new_worksheet = main_sheet.worksheet(f'{name.upper()} {file_name.lower()}')
				new_worksheet.update(values.tolist())
				done = False
				repeat -=1
			

if __name__ == "__main__":
	indexes = list(range(566))
	for index in range(566):
		result = index % 5
		if result == 0 and repeat>4:
			crawler = 0
			time.sleep(60*3)
		rand_index = np.random.choice(indexes)
		t = threading.Thread(target=njparcels,args=(rand_index ,))
		indexes.remove(rand_index)
		t.start()
worksheets = main_sheet.worksheets()
print(f"Done with {len(worksheets)} spreadsheets")
