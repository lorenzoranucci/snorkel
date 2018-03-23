import wikipedia
import os.path
import xml.etree.cElementTree as ET
import logging

def download_articles(page_titles_list, dump_file_path, lang="en"):
    logging.info("Articles download start")
    if os.path.isfile(dump_file_path):
        logging.error("Dump file already exists")
        raise Exception
    file = open(dump_file_path, 'a+')
    root=ET.Element("dump")
    wikipedia.set_lang(lang)
    for title in page_titles_list:
        try:
            page=wikipedia.page(title)
            doc = ET.SubElement(root, "document")
            ET.SubElement(doc, "id").text=page.pageid
            ET.SubElement(doc, "text").text=page.content
        except:
            print(title+" page not saved")
            logging.info(title+" page not saved")
    tree = ET.ElementTree(root)
    tree.write(file)
    logging.info("Articles download end")
