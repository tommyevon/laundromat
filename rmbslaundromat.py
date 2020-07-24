# list of dependencies for the FinsightScraper() class
from selenium import webdriver
from selenium.webdriver import ActionChains
from time import sleep
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from pandas.tseries.offsets import BDay, CustomBusinessDay
from pandas.tseries.holiday import USFederalHolidayCalendar
import sys
import csv
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import DesiredCapabilities
import concurrent.futures


class RMBS():

    class scrape():

        def __init__(self, **kwargs):

            '''
            FUNCTION: __init__
            DESC: Scraper class initializer
            PARAMETERS:
                NAME: **kwargs
                DESC: a vector containing the key-word arguments needed to properly
                initialize the Scraper class
                TYPE: vector
            RETURN VAL: void
            '''

            # grab current data for default ending target
            # KEY RISK (4) - usage of the %e flag
            self.start_date = datetime.today().strftime('%b %e, %Y')
            self.end_date = datetime.today().strftime('%b %e, %Y')

            # === instantiate global class data structures: ===
            # define account configuration settings
            # start_date and end_date default to the current date, as the most frequent
            # use of this package will be for updating the deal database daily
            self.config = {'start_date' : self.start_date,
                           'regions' : ['USOA'],
                           'type' : None,
                           'master_database_csv' : None,
                           'rawtxt' : None}

            # the time_series data structure will hold all days needed to scrape
            self.time_series = []
            self.end_date = datetime.today().strftime('%b %e, %Y')
            self.running_buffer = 1
            self.market_type = []

            for key in kwargs:
                if key not in self.config:
                    print('[CLO] WARNING: ', key, ' is an unkown argument')
                    raise KeyError('Invalid Argument Name.')

            # update the configuration settings so they now contain the passed through value.
            self.config.update(kwargs.items())

            '''
            url = 'https://finsight.com/sector/Collateralized%20Loans%20(CLOs)?products=ABS&regions=USOA'

            # initialize a chrome web browser
            # the below code, init the driver with ChromeDriverManager().install() causes my local machine to fail
            # however, this version of code would be much better if also complimented with updating chrome versions
            # self.driver = webdriver.Chrome(ChromeDriverManager().install())

            options = Options()
            options.add_argument('--headless')

            #self.driver = webdriver.Chrome(options = options)
            #self.driver.set_window_size(1920, 1080)

            self.driver = webdriver.Chrome()
            self.driver.get(url)

            self.driver= webdriver.Chrome()
            self.driver.get(url)

            # variable to keep track of how many frames are loaded in
            self.frame_count = 0

            sleep(3)
            '''

        def get_time_series(self):

            '''
            NAME: get_time_series
            DESC: this function grabs the class start and end dates as synthetic parameters, creating
            a time series of all dates the program will check to grab deals from
            PARAMETERS:
                None
            RETURN VAL: void, however, will set the class self.time_series list
            '''

            start_date = self.config['start_date']
            end_date = self.end_date

            start_date = datetime.strptime(start_date, '%b %d, %Y')
            end_date = datetime.strptime(end_date, '%b %d, %Y')

            # find the difference in days
            time_difference = end_date - start_date
            time_difference = time_difference.days

            if time_difference > 0:

                for i in range(time_difference + 1):
                    buffer_date = start_date + timedelta(days = i)
                    # KEY RISK (4) - usage of the %e flag
                    buffer_date = buffer_date.strftime('%b%e,%Y')
                    buffer_date = buffer_date.replace(' ', '')
                    self.time_series.append(buffer_date)

            elif time_difference == 0:

                start_date = start_date.strftime('%b%e,%Y').replace(' ', '')
                self.start_date = start_date

                end_date = end_date.strftime('%b%e,%Y').replace(' ', '')
                self.end_date = end_date

                self.time_series = [self.start_date, self.end_date]

        def format_deal_entry(self, deal):

            '''
            FUNCTION: format_deal_entry
            DESC: to take a raw string input containing the top-level info about a deal, and convert
            the data in that string into an easily accessible dictionary
            RETURN VALUE: a python dictionary
            PARAMETERS:
                NAME: deal
                DESC: a string containing the deal info
                TYPE: str
            RETURN VAL: python dict containing all of the top-level details of a deal, specifically
            the name, issuer, issuance date, asset class, region, size
            '''

            formatted_deal = {}

            leng = 0
            for char in deal:
                if char.isspace():
                    leng += 1

            # loop through each word of the deal, append to dict
            runner = 0
            deal_name = []
            issuer_name = []
            deal_date = []
            for word in deal.split():

                if runner >= 1 and runner < 2:
                    formatted_deal['Asset_Class'] = word

                # create a string containing the deal name
                if runner >= 2 and runner <= 3:
                    deal_name.append(word)
                    formatted_deal['Deal_Name'] = '-'.join([str(word) for word in deal_name])

                # create a string containing issuer name
                if runner >= 6 and runner <= leng - 4:
                    issuer_name.append(word)
                    formatted_deal['Issuer_Name'] = '_'.join([str(word) for word in issuer_name])

                # create a string for the region
                if runner >= leng - 4 and runner <= leng - 3:
                    formatted_deal['Region'] = word

                # create a string for the issuance date
                if runner >= leng - 2:
                    deal_date.append(word)
                    formatted_deal['Issuance_Date'] = ''.join([str(word) for word in deal_date])

                runner += 1

            try:
                deal_name = formatted_deal['Deal_Name'].split('-')
            except:
                print('problem child', deal)
                deal_name = 'BAD_DATA'
            formatted_deal['Ticker'] = deal_name[0]

            return formatted_deal

        def open_frames(self, deals):

            self.frame_count += 1

            '''
            NAME: open_frames
            DESC: open_frames recursively calls until the ending frame listing on the finsight
            website does not contain a date within the desired self.time_series. This function
            will keep loading in frames until all the frames needed are loaded on finsight.
            PARAMETERS:
                NAME: deals
                DESC: string of initial 10 top-level deal listings on the finsight page
                TYPE: str
            RETURN VAL: void, however the finsight website will have loaded in the desired amount
            of frames on the successful completion of the function
            '''

            sleep(1)

            runner = 1
            for deal in deals.splitlines():
                deal_details = self.format_deal_entry(deal)
                # if on the 10th row (end of frame) and need to load in some more
                if runner > self.running_buffer and runner % 10 == 0 and deal_details['Issuance_Date'] in self.time_series:
                    self.running_buffer = runner
                    # attmept to click the 'more' button to load in another frame of 10 deals
                    try:
                        self.driver.find_element_by_css_selector('#app > div._3gSKZ7_dwvnXrro0zBD8wm._15L--PjbsA5tVLa_svlyz1 > div > div._2IHjQGtUqFZkMDkNpIUplQ > div._2D8SD_YKh88CUSqE7rETKQ > div._2Kgdic9ImS9p6aDOvWJLcF > a').click()
                    except:
                        sleep(1)
                        self.driver.find_elements_by_xpath('//*[@id="app"]/div[2]/div/div[1]/div[3]/div[4]/a')[0].click()

                if runner > self.running_buffer and runner % 10 == 0 and deal_details['Issuance_Date'] not in self.time_series:
                    return

                runner += 1

            # open up a page on finsight containing only top-level deal details
            # KEY RISK (3)
            deals = self.driver.find_elements_by_xpath('//*[@id="app"]/div[2]/div/div[1]/div[3]/div[3]/div')[0]
            deal_listings = deals.text
            deal_listings = deal_listings.replace('ABS-15G\n', '')

            self.open_frames(deal_listings)

        def grab_deals(self):

            '''
            NAME: grab_deals
            DESC: this function, given any loaded webpage on the finsight url, scrapes all of the
            relevant deal and converts it into a massive string
            PARAMETERS: none
            RETURN VAL: str, containing all relevant deal data from finsight
            '''

            sleep(10)

            # scroll to the top of the page where the "expand" button is
            self.driver.execute_script('window.scrollTo(0, 220)')

            try:
                self.driver.find_element_by_css_selector('#app > div._3gSKZ7_dwvnXrro0zBD8wm._15L--PjbsA5tVLa_svlyz1 > div > div._2IHjQGtUqFZkMDkNpIUplQ > div._2D8SD_YKh88CUSqE7rETKQ > div._1-fgsDFyqoxpV1byNYLBhr > div').click()
            except:
                print('grab_deals error')
                sys.exit()

            i = 1
            while i <= (self.frame_count * 10):
                css_select = '#app > div._3gSKZ7_dwvnXrro0zBD8wm._15L--PjbsA5tVLa_svlyz1 > div > div._2IHjQGtUqFZkMDkNpIUplQ > div._2D8SD_YKh88CUSqE7rETKQ > div._2WeFW6hNszbKYFEyZssnGA > div > div:nth-child({}) > div.dealCardContent.dealCardContentActive > div > div._38BDLFuNtg2uSDvrBJnN8Z > a'.format(i)
                href_select = '#app > div._3gSKZ7_dwvnXrro0zBD8wm._15L--PjbsA5tVLa_svlyz1 > div > div._2IHjQGtUqFZkMDkNpIUplQ > div._2D8SD_YKh88CUSqE7rETKQ > div._2WeFW6hNszbKYFEyZssnGA > div > div:nth-child({}) > div.dealCardHeader > div > div.W8QO61jHhGdHpACNfXy4c._2nBZvHSCRJbdn5HnPV1yif > span > span > a'.format(i)
                self.driver.execute_script('window.scrollBy(0, 300)')
                sleep(0.5)
                try:
                    more_details = self.driver.find_element_by_css_selector(css_select)
                    more_details.click()
                except:
                    print('[CLO', self.config['type'], '] more_details button failure at', i)
                    i += 1
                    continue

                try:
                    mtype = self.driver.find_element_by_css_selector(css_select).get_attribute('href')
                    mtype = mtype.replace('https://finsight.com/sector/Collateralized%2520Loans%2520(CLOs)/', '')

                    if 'Broadly' in mtype:
                        self.market_type.append('BS')
                    elif 'Middle' in mtype:
                        self.market_type.append('MM')
                    elif 'Other' in mtype:
                        self.market_type.append('Other')
                    else:
                        self.market_type.append('Other(bad_data)')
                except:
                    print('[CLO] failure to collect href at', i)
                    i += 1
                    continue

                i += 1

            sleep(1)

            # KEY RISK (3)
            deals = self.driver.find_elements_by_xpath('//*[@id="app"]/div[2]/div/div[1]/div[3]/div[3]/div')[0]
            deals = deals.text
            deals = deals.replace('ABS-15G\n', '')

            return deals

        def store_data(self, raw_text):

            '''
            NAME: store_data
            DESC: given the text from the finsight web scrape, this function converts
            that string into cleanly formatted, stored data in dataframe form
            PARAMETERS:
                NAME: raw_text
                DESC: string of scraped data from the finsight website, sourced from the
                Scraper.self.grab_deals() function
                TYPE: str
            RETURN VAL: a multi-indexed python pandas dataframe containing all relevant
            finsight deal data in standardized format
            '''

            t_columns = ['Tranche', 'Currency', 'Size(M)', 'WAL', 'WALX',
                         'Moodys', 'S&P', 'Fitch', 'DBRS', 'Kroll', 'MorningStar',
                         'C/E', 'LTV', 'TYPE', 'Bench', 'Guidance-L', 'Guidance-H', 'Spread', 'Coupon',
                         'Yield', 'Issue_Price', 'Market_Type']

            deal_headers = ['CLASS', 'CCY', 'SZE(M)', 'WAL', 'MO', 'SP', 'FI', 'DR', 'KR', 'MS', 'C/E',
                            'LTV', 'TYPE', 'BNCH', 'GDNC', 'SPRD', 'CPN', 'YLD', 'PRICE']

            non_tranche_entries = ['Hide Details', 'TRANCHE COMMENTS', 'DEAL COMMENTS', 'REFINANCING',
                                   'RISK', 'RESET', 'VOLCKER', 'WAL', 'Book size', 'Non-Call Period', 'Reset',
                                   'the', 'The', 'structured', 'PRICING SPEED', 'CPR', 'REINVESTMENT', 'Reinvestment']

            master_dict = {}
            runner = 0
            details_dict = {}
            tranches_dict = {}
            row_list = []
            no_wallx = True
            clo_flag = False # flag for alerting coupon cleaner code that the current deal is a CLO
            irregular_flag = False # flag for deals that have irregular data entries
            mbs_flag = False
            point_to_tick_flag = False
            missing_data_flag = False
            missing_items = []
            deal_items_list_buffer = []

            # loop through each line of data taken from the finsight website
            for deal in raw_text.splitlines():
                # reset flags at beginning of loop
                irregular_flag = False

                # if the line item is a deal header, with top level details, grab details and put into deal entry
                if 'ABS' in deal:

                    missing_data_flag = False
                    deal_items_list_buffer = []
                    deal_items_list = []
                    # below code cleans up data pertaining specifically to CLO deals, so if the header
                    # in the pipeline is a CLO, the flag will be set so the cleaning code below can work
                    if 'CLOS' in deal:
                        clo_flag = True
                    else:
                        clo_flag = False

                    # preliminary check for deal dates. This helps catch end dates that
                    # are set to be anything other than the current date
                    if runner == 0:

                        # create dict containing top-level deal details
                        details_dict = self.format_deal_entry(deal)

                        # if current deal entry is older than start_date, skip
                        if details_dict['Issuance_Date'] not in self.time_series:
                            continue

                    # append the dataframe of deal tranches into the master dict
                    # if this is not the first run through the loop
                    if runner > 0:

                        # if current deal entry is older than start_date, skip
                        if details_dict['Issuance_Date'] not in self.time_series:
                            continue

                        # create a dataframe that zips together all of the tranches housed in row_list
                        deal_df = pd.DataFrame(row_list).set_index(['Issuance_Date', 'Asset_Class'])
                        # add the new deal dataframe to the master dictionary
                        master_dict[details_dict['Deal_Name']] = deal_df
                        row_list = []

                    # create dict containing top-level deal details
                    details_dict = self.format_deal_entry(deal)

                    runner += 1

                # if the data line contains tranche column headers

                # flag WALX issue
                elif 'CLASS' in deal:

                    missing_items = []
                    # find out what headers the deal has
                    hit_list = []
                    for i in deal.split():
                        if i in deal_headers:
                            hit_list.append(deal_headers.index(i))

                    # from what the deal has, find out what is missing
                    for i in range(0, 19):
                        if i not in hit_list:
                            missing_items.append(i)

                    # if header is intact, deal flow is standardized from finsight's end
                    if len(missing_items) > 0:
                        missing_data_flag = True

                    for item in deal.split():
                        # if the tranche indludes WALX column, set boolean
                        if item == 'WALX':
                            no_wallx = False
                            break
                        else:
                            no_wallx = True

                # parse through tranche data, add to details_dict:
                #elif any(e in deal for e in non_tranche_entries) is False

                elif 'USD' in deal:

                    # create list containing items in the deal tranche entry
                    deal_items_list = deal.split()
                    bad_DBRS_stuff = ['(H)', '(L)', 'L', 'H']
                    acceptable_price_column_values = ['Retained', 'Int.', '-']
                    trouble_items = ['bk', 'BK', 'back', 'Mid', 'mid', 'area', 'Area', 'to', 'a',
                                     'PT', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG',
                                     'SEP', 'AUG', 'NOV', 'DEC', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                     'Jul', 'Aug','Sep', 'Oct', 'Nov', 'Dec', '75%FCF', '>', '<', 'A1', 'Sup',
                                     'vs', 'A2', 'A3', 'A4', 'LCF', 'PFMT', '#', '/', 'Hi', '(the #)', '#)', '(the',
                                     '2bps', '1bps', '3bps', '4bps', '5bps', 'Low', 'High', 'minus', '$px', 'SSr',
                                     'FCF', 'l/m', '.', '->', 'yld', 'FNCI', 'FED', 'SnrSupp', 'h']
                    trouble_items_2 = ['UMBS', 'DW', 'FN', '+']
                    point_verbage = ['0-', '1-', '2-', '3-', '4-', '5-', '6-', '7-', '8-', '9-']
                    irregular_data_flag = False

                    # if the deal did not come with WALX data, add in filler WALX column data point
                    if no_wallx is True:
                        deal_items_list[4:4] = '-'

                    # put in missing data columns if applicable
                    if missing_data_flag is True:
                        for i in missing_items:
                            deal_items_list.insert(i, '-')

                    try:
                        argh = deal_items_list[19]
                    except:
                        # highly irregular data will be skipped
                        continue
                    # catch irregular data entries
                    if isinstance(deal_items_list[19], float) is True or isinstance(deal_items_list[19], int):
                        pass
                    elif deal_items_list[19] not in acceptable_price_column_values:
                        irregular_data_flag = True # key change, indent

                    # create copy to iterate over
                    #deal_items_list[:] = [e for e in deal_items_list if ]
                    deal_items_list_buffer = deal_items_list[:]

                    # initial cleaning loop, get the DBRS irregularities out, get the
                    # CLO 3mL spread modifier out, get trouble items like "back" or "area" out
                    index = 0
                    for item in deal_items_list:
                        # clean irregularities with DBRS
                        if item in bad_DBRS_stuff and index > 2:
                            # combinde DBRS modifier, '(L)' for example, and the rating
                            deal_items_list_buffer[8] = deal_items_list[8] + item
                            deal_items_list_buffer.remove(item)

                        elif irregular_data_flag is True and index > 14 and item in trouble_items and item in deal_items_list_buffer:
                            point_to_tick_flag = True
                            # check for highly irregular guidance data
                            deal_items_list_buffer.remove(item)
                            if item == 'to':
                                deal_items_list_buffer[15] = deal_items_list_buffer[15] + '-' + deal_items_list_buffer[16]
                                deal_items_list_buffer.remove(deal_items_list_buffer[16])

                        # clean up CLO "3mL+xxx" coupon to simply be int data of spread
                        elif clo_flag is True and '3mL+' in item:
                            item = item.replace('3mL+', '')
                            deal_items_list_buffer[17] = item

                        elif index > 14:
                            if item in trouble_items_2 or trouble_items_2[0] in item or trouble_items_2[1] in item or trouble_items_2[2] in item:

                                point_to_tick_flag = True
                                # check if UMBS or DW in item
                                if trouble_items_2[0] in item:
                                    deal_items_list_buffer[14] = item + '%'
                                    deal_items_list_buffer.remove(item)
                                elif trouble_items_2[1] in item:
                                    deal_items_list_buffer[14] = item + '%'
                                    deal_items_list_buffer.remove(item)
                                elif trouble_items_2[2] in item:
                                    deal_items_list_buffer[14] = item + '%'
                                    deal_items_list_buffer.remove(item)
                                else:
                                    deal_items_list_buffer.remove(item)

                        index += 1

                    # make sure list is standardized to 20 elements
                    if len(deal_items_list_buffer) > 21:
                        if deal_items_list_buffer[14] == 'IntL':
                            print(deal_items_list_buffer[14])
                            deal_items_list_buffer[16] = deal_items_list_buffer[17]
                            deal_items_list_buffer.remove(deal_items_list_buffer[16])

                    # standardize normal deal flows, setup guidance high/low
                    elif len(deal_items_list_buffer) <= 21:
                        # if there is only one guidance number, simply duplicate into high and low
                        if deal_items_list_buffer[15] == '-' or '-' not in deal_items_list_buffer[15]:
                            if deal_items_list_buffer[15] in ['Low', 'High', 'low', 'high']:
                                deal_items_list_buffer.remove(deal_items_list_buffer[15])

                            deal_items_list_buffer.insert(16, deal_items_list_buffer[15])
                        # split up a "x-y" guidance chain into high and low
                        elif '-' in deal_items_list_buffer[15]:
                            guidance_list = deal_items_list_buffer[15].split('-')
                            deal_items_list_buffer[15] = guidance_list[0]
                            deal_items_list_buffer.insert(16, guidance_list[1])

                    # catch irregularities that survived the first scub
                    try:
                        # check if a float is in the price column
                        float(deal_items_list_buffer[20])
                    except:
                        try:
                            # check if an int is in the price column
                            int(deal_items_list_buffer[20])
                        except:
                            try:
                                # catch typical irregularities
                                if irregular_data_flag is True:
                                    if mbs_flag is True and len(deal_items_list_buffer) > 21:
                                        # delete ranged guidance, just keep the floor guidance
                                        deal_items_list_buffer.remove(deal_items_list_buffer[16])
                                    elif deal_items_list_buffer[20] not in acceptable_price_column_values: # key change
                                        deal_items_list_buffer[20] = 'BAD_DATA'
                            except:
                                # catch instances where price data got cut off
                                deal_items_list_buffer.append('BAD_DATA')

                    if len(deal_items_list_buffer) > 21:
                        while len(deal_items_list_buffer) > 21:
                            deal_items_list_buffer.remove(deal_items_list_buffer[-1])

                    # zip up the t_columns (keys) and deal_items_list (values) into a tranche dictionary
                    tranches_dict = dict(zip(t_columns, deal_items_list_buffer))

                    # zip the deal details and tranche details together into one dict
                    details_dict.update(tranches_dict)
                    row_list.append(details_dict.copy())

                    point_to_tick_flag = False

            if details_dict['Issuance_Date'] in self.time_series:
            # add the last deal in the text pipeline to the dataframe dict if eligible
            # create a dataframe that zips together all of the tranches housed in row_list
                deal_df = pd.DataFrame(row_list).set_index(['Issuance_Date', 'Asset_Class'])
            # add the new deal dataframe to the master dictionary
                master_dict[details_dict['Deal_Name']] = deal_df

            # create a master dataframe that zips together all of the other deal dataframes
            master_data_set = pd.concat(master_dict.values(), keys=master_dict.keys())
            # flip the dataframe so the date goes in ascending order y-axis
            master_data_set = master_data_set[::-1]

            return master_data_set

        def store_rp_mtype(self, raw_text, dataset):

            rp_storage = {}
            details = ''

            deals = raw_text.split('ABS')
            for deal in deals:
                line_items = deal.splitlines()
                for e in line_items:
                    if 'RMBS' in e:
                        e = 'ABS ' + e
                        details = self.format_deal_entry(e)
                        details = details['Deal_Name']
                    if 'PRICING SPEED' in e:
                        info = e.split()
                        rp = info[2]
                if 'PRICING SPEED' not in deal:
                    rp = '-'

                for index, row in dataset.iterrows():
                    #print(index[0], details)
                    if index[0] == details:
                        dataset.loc[index[0], 'Market_Type'] = self.config['type']
                        dataset.loc[index[0], 'PricingSpeed'] = rp

            return dataset

        def Go(self, useless):

            # grab the relevant data dates
            self.get_time_series()

            deals = ''

            try:
                with open(self.config['rawtxt'], 'r', encoding = 'utf-8') as fin:
                    deals = fin.read()
                    deals = deals.replace('ABS-15G\n', '')
                    deals = deals.replace('ABS-15G/A\n', '')
            except:
                print('error opening', self.config['rawtxt'])
                return

            master_data_set = self.store_data(deals)

            t_columns = ['Deal_Name', 'Region', 'Ticker', 'Tranche', 'Currency',
                         'Size(M)', 'WAL', 'WALX', 'Moodys', 'S&P', 'Fitch', 'DBRS', 'Kroll', 'MorningStar',
                         'C/E', 'LTV', 'TYPE', 'Bench', 'Guidance-L', 'Guidance-H', 'Spread', 'Coupon',
                         'Yield', 'Issue_Price', 'PricingSpeed', 'Market_Type']

            master_data_set = master_data_set.reindex(columns = t_columns)
            master_data_set = self.store_rp_mtype(raw_text = deals, dataset = master_data_set)

            return master_data_set

    def __init__(self, **kwargs):

        self.start_date = datetime.today().strftime('%b %e, %Y')
        self.end_date = datetime.today().strftime('%b %e, %Y')

        self.config = {'start_date' : self.start_date,
                       'master_database_csv' : None}

        for key in kwargs:
            if key not in self.config:
                print('[CLO] WARNING: ', key, ' is an unkown argument')
                raise KeyError('Invalid Argument Name.')

        # update the configuration settings so they now contain the passed through value.
        self.config.update(kwargs.items())

    def Go(self):

        import queue
        from threading import Thread

        que = queue.Queue()
        threads_list = []

        uk = RMBS.scrape(start_date = 'Nov 19, 2012', rawtxt = 'RMBS_UK_raw_historical.txt', type = 'UK')
        sfr = RMBS.scrape(start_date = 'Oct 31, 2013', rawtxt = 'RMBS_SFR_raw_historical.txt', type = 'SFR')
        serva = RMBS.scrape(start_date = 'Jun 2, 2009', rawtxt = 'RMBS_ServAdv_raw_historical.txt', type = 'ServAdv')
        perf = RMBS.scrape(start_date = 'Jun 24, 2013', rawtxt = 'RMBS_Perf_raw_historical.txt', type = 'Performing')
        nonperf = RMBS.scrape(start_date = 'Jul 18, 2011', rawtxt = 'RMBS_NonPerf_raw_historical.txt', type = 'NonPerforming')
        aus = RMBS.scrape(start_date = 'Jul 10, 2010', rawtxt = 'RMBS_AUS_raw_historical.txt', type = 'AUS')
        ptpj = RMBS.scrape(start_date = 'Jan 15, 2013', rawtxt = 'RMBS_PTPJ_raw_historical.txt', type = 'PassThroughPrimeJumbo')
        crt = RMBS.scrape(start_date = 'Jul 23, 2013', rawtxt = 'RMBS_CRT_raw_historical.txt', type = 'CRT')
        resec = RMBS.scrape(start_date = 'Oct 10, 2014', rawtxt = 'RMBS_ReSec_raw_historical.txt', type = 'Resecuritized')

        t = Thread(target = lambda q, arg1: q.put(uk.Go(arg1)), args=(que, 'junk'))
        t.start()
        t1 = Thread(target = lambda q, arg1: q.put(sfr.Go(arg1)), args=(que, 'junk'))
        t1.start()
        t2 = Thread(target = lambda q, arg1: q.put(serva.Go(arg1)), args=(que, 'junk'))
        t2.start()
        t3 = Thread(target = lambda q, arg1: q.put(perf.Go(arg1)), args=(que, 'junk'))
        t3.start()
        t4 = Thread(target = lambda q, arg1: q.put(nonperf.Go(arg1)), args=(que, 'junk'))
        t4.start()
        t5 = Thread(target = lambda q, arg1: q.put(aus.Go(arg1)), args=(que, 'junk'))
        t5.start()
        t6 = Thread(target = lambda q, arg1: q.put(ptpj.Go(arg1)), args=(que, 'junk'))
        t6.start()
        t7 = Thread(target = lambda q, arg1: q.put(crt.Go(arg1)), args=(que, 'junk'))
        t7.start()
        t8 = Thread(target = lambda q, arg1: q.put(resec.Go(arg1)), args=(que, 'junk'))
        t8.start()

        threads_list.append(t)
        threads_list.append(t1)
        threads_list.append(t2)
        threads_list.append(t3)
        threads_list.append(t4)
        threads_list.append(t5)
        threads_list.append(t6)
        threads_list.append(t7)
        threads_list.append(t8)

        for t in threads_list:
            t.join()

        list_of_df = []

        # Check thread's return value
        while not que.empty():
            result = que.get()
            list_of_df.append(result)

        print('length of df:', len(list_of_df))
        odf = list_of_df[0]
        odf = odf.append(list_of_df[1])
        odf = odf.append(list_of_df[2])
        odf = odf.append(list_of_df[3])
        odf = odf.append(list_of_df[4])
        odf = odf.append(list_of_df[5])
        odf = odf.append(list_of_df[6])
        odf = odf.append(list_of_df[7])
        odf = odf.append(list_of_df[8])

        # convert out of multi-index into single index
        odf = odf.reset_index(level=[1,2])

        odf['Issuance_Date'] = pd.to_datetime(odf.Issuance_Date, format = '%b%d,%Y')
        odf = odf.sort_values(by = 'Issuance_Date')
        #odf.sort('Issuance_Date')

        odf.to_csv('rmbs_master_database.csv', index = True)

bruh = RMBS().Go()
