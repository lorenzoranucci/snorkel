import wikipedia
import os.path
import xml.etree.cElementTree as ET
import logging

def download_articles(page_titles_list, dump_folder_path, lang="en"):
    logging.info("Articles download start")
    wikipedia.set_lang(lang)
    for title in page_titles_list:
        filename = title.encode('ascii',errors='ignore').replace(" ","_")+".xml"
        dump_file_path=dump_folder_path+filename
        if os.path.isfile(dump_file_path):
            logging.error(title+": Dump file already exists")
            continue
        file = open(dump_file_path, 'a+')
        try:
            page=wikipedia.page(title)
            root=ET.Element("documents")
            doc = ET.SubElement(root, "doc")
            doc.set('title', page.title)
            doc.text=page.content
            tree = ET.ElementTree(root)
            tree.write(file)
        except:
            print(title+" page not saved")
            logging.info(title+" page not saved")

    logging.info("Articles download end")
