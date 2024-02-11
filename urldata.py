import urllib.request
import xml.etree.ElementTree as ET

# URL of the XML data
url = "https://informo.madrid.es/informo/tmadrid/pm.xml"

# Make an HTTP GET request and read the response
with urllib.request.urlopen(url) as response:
    xml_str = response.read()

# Parse the XML content
root = ET.fromstring(xml_str)

# Extract and print details for each 'pm' element
for pm in root.findall('pm'):
    print("ID:", pm.find('idelem').text)
    print("Description:", pm.find('descripcion').text)
    print("Associated Access:", pm.find('accesoAsociado').text)
    print("Intensity:", pm.find('intensidad').text)
    print("Occupation:", pm.find('ocupacion').text)
    print("Load:", pm.find('carga').text)
    print("Service Level:", pm.find('nivelServicio').text)
    print("Saturation Intensity:", pm.find('intensidadSat').text)
    print("Error:", pm.find('error').text)
    print("Subarea:", pm.find('subarea').text)
    print("X Coordinate:", pm.find('st_x').text)
    print("Y Coordinate:", pm.find('st_y').text)
    print()
