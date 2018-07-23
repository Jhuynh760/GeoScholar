import sys
import os
import arcpy
from collections import defaultdict
import pandas as pd
import numpy as np
import arcgis
from arcgis.gis import GIS
from arcgis.geocoding import batch_geocode
from google.cloud import language
from scholarly import scholarly
import re
try:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.path.dirname(os.path.realpath(__file__)), "GeoScholar-26c15379dc9b.json")
except Exception:
    pass


class scholar_processor:
    def __init__(self, search_query):
        self.search_query = search_query
        # self.pub_generator = scholarly.search_keyword(self.search_query)
        self.pid = 0
        self.pub_oid = 0
        self.author_oid = 0
        self.author_attr_tbl = defaultdict()
        self.pub_attr_tbl = defaultdict()
        self.publication_list = []
        self.entity_type = ('UNKNOWN', 'PERSON', 'LOCATION', 'ORGANIZATION', 'EVENT', 'WORK_OF_ART', 'CONSUMER_GOOD', 'OTHER')
        self.df = pd.DataFrame(columns = ["OID", "PID", "INSTIT_NAME", "STUDY_NAME", "AUTHOR_NAME", "CITATION_NUM", "PUBL_LINK", "PUB_CITATION", "O_AUTHORS", "STUDY_YEAR"])
        self.pub_df = pd.DataFrame(columns = ["OID", "PID", "AUTHORS", "PUBL_LINK", "PUB_CITATION", "STUDY_YEAR", "STUDY_NAME", "STUDY_LOC"])

#publication attr: OID, PID, list of authors, Link to pub, times cited, year, name
    def SwapFirstLastName(self, auth_str):
        auth_str = auth_str
        auth_str = re.sub("(,{1} +)|( +,{1} {1})|( +,{1} +)", ",", auth_str)
        auth_str = re.sub("(a{1}n{1}d{1})", "", auth_str)
        auth_str = auth_str.split(" ")
        auth_str = [nm for nm in auth_str if nm != ""]
        name_string = []
        index_name_string = 0
        while_loop = 0
        while while_loop < len(auth_str):
            temp = auth_str[while_loop].split(",")
            # print("NAME: {} LEN: {}".format(temp, len(temp)))
            if len(temp) == 1:
                if ((while_loop + 1) < len(auth_str)):
                    next_temp = auth_str[while_loop+1].split(",")
                    # print("NEXT_TEMP {} LEN: {}".format(next_temp, len(next_temp)))
                    if len(next_temp) == 1:
                        name_string.append("{} {}".format(temp[0], next_temp[0]))
                        while_loop += 2
                    else:
                        # print("NAME: {}".format(temp))
                        name_string.append("{}".format(temp[0]))
                        while_loop += 1
                else:
                    name_string.append("{}".format(temp[0]))
                    while_loop += 1
            else:
                if ((while_loop + 1) < len(auth_str)):
                    next_temp = auth_str[while_loop+1].split(",")
                    # print("NEXT_TEMP {} LEN: {}".format(next_temp, len(next_temp)))
                    if len(next_temp) == 1:
                        name_string.append("{} {} {}".format(temp[1], next_temp[0], temp[0]))
                        while_loop += 2
                    else:
                        # print("NAME: {}".format(temp))
                        name_string.append("{} {}".format(temp[1], temp[0]))
                        while_loop += 1
                else:
                    name_string.append("{} {}".format(temp[1], temp[0]))
                    while_loop += 1
        listed_authors = name_string
        name_string = " and ".join(name_string) 
        return (listed_authors, name_string)

    def ProcessAuthors(self, authors: "Author_String"):
        self.language_client = language.LanguageServiceClient()
        document = language.types.Document(content = authors,
            language = 'en',
            type=language.enums.Document.Type.PLAIN_TEXT)

        entity_list = []
        analysis = self.language_client.analyze_entities(document=document,encoding_type='UTF32')
        for entity in analysis.entities:
            if  (self.entity_type[entity.type] == self.entity_type[1]) or (self.entity_type[entity.type] == self.entity_type[3]):
                entity_list.append((entity.name, self.entity_type[entity.type], entity.salience))
            # arcpy.AddMessage('=' * 20)
            # arcpy.AddMessage('         name: {0}'.format(entity.name))
            # arcpy.AddMessage('         type: {0}'.format(self.entity_type[entity.type]))
            # # arcpy.AddMessage('     metadata: {0}'.format(entity.metadata))
            # arcpy.AddMessage('     salience: {0}'.format(entity.salience))
        entity_list.sort(key=lambda tuple: tuple[2], reverse=True)
        # print("ENTITY LIST: {}".format(entity_list))
        return entity_list
    def GetAuthorInstitutionAndCitations(self, author_name):
        search_query = scholarly.search_author(author_name)
        query = next(search_query)
        # print("QUERY: {}".format(query))
        return (query.affiliation, query.citedby)

    def ProcessPublication(self, OID, PID, PUB_OID, publication):
        bib = publication.bib
        listed_authors, author_string = self.SwapFirstLastName(bib["author"]) #return (listed_authors, name_string)
        # print("LISTED_AUTHORS: {}\nAUTHOR_SRING: {}".format(listed_authors, author_string))
        author_entity_list = self.ProcessAuthors(author_string)
        OID = OID
        PID = PID
        PUB_OID = PUB_OID
        try:
            PUBLICATION_LINK = bib['url'] #STRING
        except Exception:
            PUBLICATION_LINK = ""
        try:
            STUDY_YEAR = bib["year"]
        except Exception:
            STUDY_YEAR = 0
        for author in author_entity_list:
            # print("ENTITY_LIST: {}".format(author_entity_list))
            # print("AUTHOR: {}".format(author))
            temp_list = listed_authors
            # print("TEMP_LIST: {}".format(temp_list))
            try:
                temp_list.remove(author[0])
            except Exception:
                pass
            o_auth_str = " and ".join(temp_list)
            try:
                instit_name, cited = self.GetAuthorInstitutionAndCitations(author[0])
            except:
                instit_name = "unknown"
                cited = 0
            if instit_name != "unknown":
                attributes = [
                    ("OID", OID),
                    ("PID", PID),
                    ("INSTIT_NAME", instit_name), #STRING
                    ("STUDY_NAME", bib["title"]), #STRING
                    ("AUTHOR_NAME" , author[0]),  #STRING
                    ("CITATION_NUM", cited), #INT
                    ("PUBL_LINK", PUBLICATION_LINK),
                    ("PUB_CITATION", publication.citedby), #INT
                    ("O_AUTHORS", o_auth_str), #STRING
                    ("STUDY_YEAR", STUDY_YEAR)]
                pub_dict = defaultdict()
                for attr in attributes:
                    pub_dict[attr[0]] = attr[1]
                self.df.loc[OID] = pub_dict
                self.author_oid += 1
                OID += 1

    def geocode_Institution_table(self, table, outputname):
        institutions = table['INSTIT_NAME'].tolist()
        # print("INSTITUTION_NAMES: {}".format(institutions))
        gis = GIS(username = "<INSERT_USERNAME>", password="<INSERTPASSWORD>")
        OGinstit = []
        processed_instit = []
        for instit in institutions:
            OGinstit.append(instit)
            processed_instit.append(self.process_institution(instit))
        # print(processed_instit)
        i = 0
        len_processed_instit = len(processed_instit)
        for instit in OGinstit:
            temp = None
            if ((i+1) < len_processed_instit):
                temp = {"INSTIT_NAME": instit, "data": processed_instit[i]}
                i+=1
            else:
                temp = {"INSTIT_NAME": instit, "data": processed_instit[i]}
                break
            def update_value(row, data=temp):
                if row.INSTIT_NAME == data["INSTIT_NAME"]:
                    row.INSTIT_NAME = data["data"]
            table.apply(update_value, axis=1)
        geocode_result = batch_geocode(addresses = processed_instit)
        # print("GEOCODE_RESULT: {}".format(geocode_result))
        geocode_dict = {"x": [], "y":[]}
        table["x"] = 0
        table["y"] = 0
        for result in geocode_result:
            loc = result["location"]
            print("X: {} Y: {}".format(loc["x"], loc["y"]))
            geocode_dict["x"].append(loc["x"])
            geocode_dict["y"].append(loc["y"])
        x_ind = 0
        y_ind = 0
        for index, row in table.iterrows():
            row_copy = row
            table.loc[index, "x"] = geocode_dict["x"][x_ind]
            table.loc[index, "y"] = geocode_dict["y"][y_ind]
            x_ind+=1
            y_ind+=1

#csv_geocode
#csv_upload.ipynb
#append data to feature layer
#create an empty layer and then updtaing it
#overwrite weblayer in python api delete features delete 1 = 1

    def process_institution(self, institution):
        self.language_client = language.LanguageServiceClient()
        document = language.types.Document(content = institution,
            language = 'en',
            type=language.enums.Document.Type.PLAIN_TEXT)
        analysis = self.language_client.analyze_entities(document=document,encoding_type='UTF32')
        ent_list = []
        for entity in analysis.entities:
            instit = "Unknown"
            sal = 0
            if  (self.entity_type[entity.type] == self.entity_type[2]):
                instit = entity.name
                sal = entity.salience
                ent_list.append((instit, sal))
        ent_list.sort(key=lambda tuple: tuple[1], reverse=True)
        try:
            return ent_list[0][0]
        except Exception:
            return "Unknown"

    def geocode_publication_table(self, table, outputname):
        study_locations = table['STUDY_LOC'].tolist()
        # print("INSTITUTION_NAMES: {}".format(institutions))
        gis = GIS(username = "<INSERT_USERNAME>", password="<INSERTPASSWORD>")

        geocode_result = batch_geocode(addresses = study_locations)
        # print("GEOCODE_RESULT: {}".format(geocode_result))
        geocode_dict = {"x": [], "y":[]}
        table["x"] = 0
        table["y"] = 0
        for result in geocode_result:
            loc = result["location"]
            # print("X: {} Y: {}".format(loc["x"], loc["y"]))
            geocode_dict["x"].append(loc["x"])
            geocode_dict["y"].append(loc["y"])
        x_ind = 0
        y_ind = 0
        for index, row in table.iterrows():
            row_copy = row
            table.loc[index, "x"] = geocode_dict["x"][x_ind]
            table.loc[index, "y"] = geocode_dict["y"][y_ind]
            x_ind+=1
            y_ind+=1

    def CreateAttrTables(self):
        self.pub_generator = scholarly.search_pubs_query(self.search_query)
        for iteration in range(0, 50):
            publication = next(self.pub_generator).fill()
            try:
                abstract = publication.bib["abstract"]
            except:
                continue
            fixed_names = self.SwapFirstLastName(publication.bib["author"])
            study_loc = self.ProcessAbstract(abstract)
            self.ProcessPublication(self.author_oid, self.pid, self.pub_oid, publication)
            try:
                PUBLICATION_LINK = publication.bib['url'] #STRING
            except Exception:
                PUBLICATION_LINK = ""
            try:
                STUDY_YEAR = publication.bib["year"]
            except Exception:
                STUDY_YEAR = 0
            data = {"OID": self.pub_oid, "PID": self.pid, "AUTHORS" : fixed_names[1], "PUBL_LINK" : PUBLICATION_LINK, "PUB_CITATION" : publication.citedby, "STUDY_YEAR": STUDY_YEAR, "STUDY_NAME": publication.bib["title"], "STUDY_LOC": study_loc}
            self.pub_df.loc[self.pub_oid] = data
            print("FINISHED PUBLICATION: {}".format(self.pid))
            self.pid +=1
            self.pub_oid += 1

            


#["OID", "PID", "AUTHORS", "PUBL_LINK", "PUB_CITATION", "STUDY_YEAR", "STUDY_NAME"]
    def ProcessAbstract(self, abstract):
        self.language_client = language.LanguageServiceClient()
        document = language.types.Document(content = abstract,
            language = 'en',
            type=language.enums.Document.Type.PLAIN_TEXT)
        entity_list = []
        analysis = self.language_client.analyze_entities(document=document,encoding_type='UTF32')
        for entity in analysis.entities:
            if  (self.entity_type[entity.type] == self.entity_type[2]):
                entity_list.append((entity.name, entity.salience))
        entity_list.sort(key=lambda tuple: tuple[1], reverse=True)
        try:
            return entity_list[0][0]
        except Exception:
            return "Unknown"

    def routine(self, topic): #return two feature layers
        try:
            self.CreateAttrTables()
            self.geocode_Institution_table(self.df, "Authors")
            self.geocode_publication_table(self.pub_df, "Publication")
            topic = topic.replace(" ", "")
        finally:
            self.df.to_csv("{}Authors.csv".format(topic), encoding = "utf-8-sig")
            self.pub_df.to_csv("{}Publications.csv".format(topic), encoding = "utf-8-sig")
        # print("#########################DF TABLE###################")
        # print(self.df)

if __name__ == '__main__':
    # search_query = arcpy.GetParameter(0)
    # search_query = "Malaria"
    # arcpy.AddMessage("INPUT: {}".format(search_query))
    # list_of_topics = ["Malaria", "Opioid Crisis", "Mental Health", "College Education Rates"]
    
    list_of_topics = ["Malaria"]#, "College Education Rates"]
    for topic in list_of_topics:
        process = scholar_processor(topic)
        process.routine(topic)
    # pass

