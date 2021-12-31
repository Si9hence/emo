# import requests
from requests_html import HTMLSession

# url = 'https://exomol.com/data/molecules/NaO/23Na-16O/NaOUCMe/'
url = 'https://exomol.com/data/molecules/'
s = HTMLSession()
r = s.get(url)
t = r.html.find('div.grid-item')[0].text
print(t)