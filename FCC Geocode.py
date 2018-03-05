import pandas as pd
import requests
import logging
import time

logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

# ---------------- CONFIGURATION ---------------------------

# Backoff time sets how many minutes to wait between FCC pings when your API limit is hit
BACKOFF_TIME = 30
# Set your output file here.
output_filename = '/Users/XXXXXX_2.csv'
# Set your input file name here.
input_filename = '/Users/XXXXXX_1.csv'
# Specify the colun name in your input data that contains latlong here
lat_column_name = "Latitude"
long_column_name = "Longitude"

# Return Full FCC Results? If TRUE, full JSON results from FCC are included in output
RETURN_FULL_RESULTS = False

# -------------------------- DATA LOADING -----------------------

# Read the data to a Pandas DataFrame
data = pd.read_csv(input_filename, encoding='utf-8')

if lat_column_name not in data.columns:
    raise ValueError("Missing Latitude column in input data")

if long_column_name not in data.columns:
    raise ValueError("Missing Longitude column in input data")


# Form a list of Lat/Long for geocoding:
# Make a big list of all of the latlong to be processed.
latitudes = data[lat_column_name].tolist()
longitudes = data[long_column_name].tolist()


#------------------ FUNCTION DEFINITIONS ------------------------

def get_fcc_results(latitude,longitude,showall=False):
    """
    Get geocode results from FCC API.

    Note, that in the case of multiple FCC geocode results, this function returns details of the FIRST result.

    @param return_full_response: Boolean to indicate if you'd like to return the full response from google. This
                    is useful if you'd like additional location details for storage or parsing later.
    """
    # Set up your Geocoding url
    geocode_url = "http://data.fcc.gov/api/block/find?format=json&latitude={}&longitude={}".format(latitude,longitude)


   # Ping FCC for the reuslts:
    results = requests.get(geocode_url)
    # Results will be in JSON format - convert to dict using requests functionality
    results = results.json()

    # if there's no results or an error, return empty results.
    if len(results['Block']) == 0:
        output = {
            "FIPS" : None
        }
    else:
        answer = results['Block']
        output = {
            "FIPS": answer.get('FIPS')
        }

    # Append some other details:
    output['latitudes'] = latitude
    output['longitudes'] = longitude

    # output['number_of_results'] = len(results['results'])
    output['status'] = results.get('status')
    if showall is True:
        output['response'] = results

    return output

#------------------ PROCESSING LOOP -----------------------------

# Ensure, before we start, that the internet access is ok
test_result = get_fcc_results("40.752726", "-73.977229", RETURN_FULL_RESULTS)
if (test_result['status'] != 'OK') or (test_result['FIPS'] != '360610092001007'):
    logger.warning("There was an error when testing the FCC Geocoder.")
    raise ConnectionError('Problem with test results from FCC Geocode - check your data format and internet connection.')

# Create a list to hold results
results = []
# Go through each lat-long in turn
for latitude, longitude in zip(latitudes, longitudes):
    # While the address geocoding is not finished:
    geocoded = False
    while geocoded is not True:
        # Geocode the address with FCC
        try:
            geocode_result = get_fcc_results(latitude,longitude, showall=RETURN_FULL_RESULTS)
        except Exception as e:
            logger.exception(e)
            logger.error("Major error with {} and {}".format(latitude, longitude))
            logger.error("Skipping!")
            geocoded = True

        # If we're over the API limit, backoff for a while and try again later.
        if geocode_result['status'] == 'OVER_QUERY_LIMIT':
            logger.info("Hit Query Limit! Backing off for a bit.")
            time.sleep(BACKOFF_TIME * 60) # sleep for 30 minutes
            geocoded = False
        else:
            # If we're ok with API use, save the results
            # Note that the results might be empty / non-ok - log this
            if geocode_result['status'] != 'OK':
                logger.warning("Error geocoding {}, {}: {}".format(latitude, longitude, geocode_result['status']))
            logger.debug("Geocoded: {}, {}: {}".format(latitude, longitude, geocode_result['status']))
            results.append(geocode_result)
            geocoded = True

    # Print status every 1000 latlong
    if len(results) % 1000 == 0:
      logger.info("~~~~~~~~COMPLETED {} OF {} LAT-LONG PAIRS~~~~~~~~".format(len(results), len(latitudes)))

    # Every 50000 latlong, save progress to file(in case of a failure so you have something!)
    if len(results) % 50000 == 0:
        pd.DataFrame(results).to_csv("{}_bak".format(output_filename))

# All done
logger.info("~~~~~~~~FINISHED GEOCODING ALL LAT-LONG PAIRS~~~~~~~~")
# Write the full results to csv using the pandas library.
pd.DataFrame(results).to_csv(output_filename, encoding='utf8')
