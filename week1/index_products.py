# From https://github.com/dshvadskiy/search_with_machine_learning_course/blob/main/index_products.py
import opensearchpy
import requests
from lxml import etree

import click
import glob
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import logging

from time import perf_counter
import concurrent.futures



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format='%(levelname)s:%(message)s')

# NOTE: this is not a complete list of fields.  If you wish to add more, put in the appropriate XPath expression.
#TODO: is there a way to do this using XPath/XSL Functions so that we don't have to maintain a big list?
mappings =  [
            "productId/text()", "productId",
            "sku/text()", "sku",
            "name/text()", "name",
            "type/text()", "type",
            "startDate/text()", "startDate",
            "active/text()", "active",
            "regularPrice/text()", "regularPrice",
            "salePrice/text()", "salePrice",
            "artistName/text()", "artistName",
            "onSale/text()", "onSale",
            "digital/text()", "digital",
            "frequentlyPurchasedWith/*/text()", "frequentlyPurchasedWith",# Note the match all here to get the subfields
            "accessories/*/text()", "accessories",# Note the match all here to get the subfields
            "relatedProducts/*/text()", "relatedProducts",# Note the match all here to get the subfields
            "crossSell/text()", "crossSell",
            "salesRankShortTerm/text()", "salesRankShortTerm",
            "salesRankMediumTerm/text()", "salesRankMediumTerm",
            "salesRankLongTerm/text()", "salesRankLongTerm",
            "bestSellingRank/text()", "bestSellingRank",
            "url/text()", "url",
            "categoryPath/*/name/text()", "categoryPath", # Note the match all here to get the subfields
            "categoryPath/*/id/text()", "categoryPathIds", # Note the match all here to get the subfields
            "categoryPath/category[last()]/id/text()", "categoryLeaf",
            "count(categoryPath/*/name)", "categoryPathCount",
            "customerReviewCount/text()", "customerReviewCount",
            "customerReviewAverage/text()", "customerReviewAverage",
            "inStoreAvailability/text()", "inStoreAvailability",
            "onlineAvailability/text()", "onlineAvailability",
            "releaseDate/text()", "releaseDate",
            "shippingCost/text()", "shippingCost",
            "shortDescription/text()", "shortDescription",
            "shortDescriptionHtml/text()", "shortDescriptionHtml",
            "class/text()", "class",
            "classId/text()", "classId",
            "subclass/text()", "subclass",
            "subclassId/text()", "subclassId",
            "department/text()", "department",
            "departmentId/text()", "departmentId",
            "bestBuyItemId/text()", "bestBuyItemId",
            "description/text()", "description",
            "manufacturer/text()", "manufacturer",
            "modelNumber/text()", "modelNumber",
            "image/text()", "image",
            "condition/text()", "condition",
            "inStorePickup/text()", "inStorePickup",
            "homeDelivery/text()", "homeDelivery",
            "quantityLimit/text()", "quantityLimit",
            "color/text()", "color",
            "depth/text()", "depth",
            "height/text()", "height",
            "weight/text()", "weight",
            "shippingWeight/text()", "shippingWeight",
            "width/text()", "width",
            "longDescription/text()", "longDescription",
            "longDescriptionHtml/text()", "longDescriptionHtml",
            "features/*/text()", "features" # Note the match all here to get the subfields

        ]

def get_opensearch():
    host = 'localhost'
    port = 9200
    auth = ('admin', 'admin')
    client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_compress=True,
        http_auth=auth,
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
    )
    return client


def index_file(file, index_name):
    docs_indexed = 0
    client = get_opensearch()
    logger.info(f'Processing file : {file}')
    tree = etree.parse(file)
    root = tree.getroot()
    children = root.findall("./product")
    docs = []
    for child in children:
        doc = {
    "productId": "12345",
    "sku": "SKU12345",
    "name": "Sample Product",
    "type": "Electronics",
    "startDate": "2023-01-01",
    "active": "true",
    "regularPrice": "299.99",
    "salePrice": "279.99",
    "artistName": "Artist XYZ",
    "onSale": "false",
    "digital": "false",
    "frequentlyPurchasedWith": ["ProductA", "ProductB"],
    "accessories": ["Accessory1", "Accessory2"],
    "relatedProducts": ["ProductC", "ProductD"],
    "crossSell": "ProductE",
    "salesRankShortTerm": "5",
    "salesRankMediumTerm": "15",
    "salesRankLongTerm": "30",
    "bestSellingRank": "10",
    "url": "http://example.com/product",
    "categoryPath": ["Electronics", "Gadgets"],
    "categoryPathIds": ["123", "456"],
    "categoryLeaf": "789",
    "categoryPathCount": "2",
    "customerReviewCount": "100",
    "customerReviewAverage": "4.5",
    "inStoreAvailability": "true",
    "onlineAvailability": "true",
    "releaseDate": "2023-02-01",
    "shippingCost": "0.00",
    "shortDescription": "A brief description of the product.",
    "shortDescriptionHtml": "<p>A brief description of the product.</p>",
    "class": "Electronics",
    "classId": "001",
    "subclass": "Gadgets",
    "subclassId": "002",
    "department": "Tech",
    "departmentId": "003",
    "bestBuyItemId": "BB12345",
    "description": "A detailed description of the product.",
    "manufacturer": "TechCorp",
    "modelNumber": "TC12345",
    "image": "http://example.com/image.jpg",
    "condition": "New",
    "inStorePickup": "true",
    "homeDelivery": "true",
    "quantityLimit": "5",
    "color": "Black",
    "depth": "5 inches",
    "height": "10 inches",
    "weight": "1.5 lbs",
    "shippingWeight": "2 lbs",
    "width": "3 inches",
    "longDescription": "A very detailed description of the product.",
    "longDescriptionHtml": "<p>A very detailed description of the product.</p>",
    "features": ["Feature1", "Feature2"]
}

        for idx in range(0, len(mappings), 2):
            xpath_expr = mappings[idx]
            key = mappings[idx + 1]
            doc[key] = child.xpath(xpath_expr)
        #print(doc)
        if 'productId' not in doc or len(doc['productId']) == 0:
            docs.append({'_index': index_name, '_source': doc})
        
        # Bulk index every 2000 documents
        if len(docs) == 2000:
            bulk(client, docs, request_timeout=60)
            docs_indexed += len(docs)
            docs = []

    # Index any remaining documents
    if docs:
        docs.append(the_doc)
        bulk(client, docs, request_timeout=60)
        docs_indexed += len(docs)

    return docs_indexed

@click.command()
@click.option('--source_dir', '-s', help='XML files source directory')
@click.option('--index_name', '-i', default="bbuy_products", help="The name of the index to write to")
@click.option('--workers', '-w', default=8, help="The number of workers to use to process files")
def main(source_dir: str, index_name: str, workers: int):

    files = glob.glob(source_dir + "/*.xml")
    docs_indexed = 0
    start = perf_counter()
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(index_file, file, index_name) for file in files]
        for future in concurrent.futures.as_completed(futures):
            docs_indexed += future.result()

    finish = perf_counter()
    logger.info(f'Done. Total docs: {docs_indexed} in {(finish - start)/60} minutes')

if __name__ == "__main__":
    main()