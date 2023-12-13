


# ETL PIPELINES:


# SIPSA PRICES PIPELINES:
PRODUCTS_SIPSA_CODES ={
            'aguacate_hass': '89',
            'aguacate_papelillo': '90',
            'naranja_valencia': '86',
            'limon_tahiti': '81',
            'platano_harton_primera': '189',
            'platano_harton_extra': '187',
            'maracuya': '133',
            'papaya tainung': '6002',
            'lulo': '116',
            'papa parda pastusa': '167',
            'cebolla cabezona blanca': '18',
            'piña gold': '144',
            'zanahoria': '54'
        }

SIPSA_REQUEST_HEADERS = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'es-419,es;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Cookie': 'ec1d9c9ecb5c2f4ef7998e3e52b19fdf=pbves1lfevgso9iovbp41e7slm; _gid=GA1.3.312679753.1690809092; SERVERID=s1; cookiesession1=678B76A19A61DAD67B01DCF9AA04C524; PRETSESSID=jWBPjosQS5%2CryoIf9-OXcE3QZq7i2ucy8c-1; _ga=GA1.1.1541742356.1690214816; _ga_EVNW3DW2NE=GS1.1.1691092597.28.1.1691095862.50.0.0; _ga_MV4DN0WN4F=GS1.1.1691092597.28.1.1691095862.0.0.0',
            'Origin': 'https://sen.dane.gov.co',
            'Referer': 'https://sen.dane.gov.co/variacionPrecioMayoristaSipsa_Client/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"'
            }

SIPSA_API_URL = 'https://sen.dane.gov.co/variacionPrecioMayoristaSipsa_ws/rest/SipsaServices/selectAllInfoProduct/'