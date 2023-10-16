class PropertyDTO:
    def __init__(self, price, image_urls, bedrooms, bathrooms, size_sqft, address, url, date, success,
    operation_type, property_type, agency_name, year):
        self.price = price
        self.image_urls = image_urls
        self.bedrooms = bedrooms
        self.bathrooms = bathrooms
        self.size_sqft = size_sqft
        self.address = address
        self.url = url
        self.date = date
        self.success = success
        self.operation_type = operation_type
        self.property_type = property_type
        self.agency_name = agency_name
        self.year = year