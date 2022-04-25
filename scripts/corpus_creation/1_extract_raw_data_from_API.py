#!/usr/bin/env python
# coding: utf-8

# AFTONBLADET

import re
import random
import json
import pandas as pd
from kblab import Archive
from json import load
import csv
from kblab.utils import flerge
import numpy as np
import glob


auth = (username ,password)
a = Archive('https://datalab.kb.se', auth = auth)


random.seed(901207)


with open("/home/r3/Documents/full_data_20191118/dates_afb.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter = ',', )
    next(csv_reader, None)
    for row in csv_reader:
        newspaper_days = [row[0] + ' ' + row[1]]
        for ds in range(len(newspaper_days)):
            newspaper_day = newspaper_days[ds]
            newspaper_day = ' '.join(newspaper_day.split())
            packid = []
            for package_id in a.search({"label": newspaper_day}):
                try: 
                    p = a.get(package_id)
                    if re.search(re.compile('dark-'), str(p)):
                        packid.append(package_id)
                except:
                    print('Cannot load package')
                    
                    
            # DUE TO BUG IN API, HAVE TO MATCH TITLE WITH SEARCH STRING

            if len(packid)>=1:
                packid2 = []
                for pi in packid:
                    p = a.get(pi)
                    for fname in p:
                        if p[fname]['@type'] == 'Meta':
                            try:
                                meta_data = load(p.get_raw('meta.json'))
                                title = meta_data['title']
                                if title.replace(" ", "") == newspaper_day.replace(" ", ""):
                                    packid2.append(pi)
                            except:
                                 print('Cannot load Meta data for: ' + newspaper_day)
                                    
             # IF MORE THAN ONE EDITION, RANDOMLY PICK ONE                       
                             
                    if len(packid2)>1: 
                        random_upplaga = random.choice(packid2)
                        random_upplaga_D = 1
                    elif len(packid2) == 0:
                        random_upplaga = 'NaN'
                    else:
                        random_upplaga = packid2[0]
                        random_upplaga_D = 0
         
                            
            else:
                random_upplaga = 'NaN'
                    
    
            # IF EDITION EXISTS, EXTRACT STRUCTURE AND CONTENT INFO
        
            if random_upplaga != 'NaN':
                p = a.get(random_upplaga)

                packageID = []
                title = []  
                date = []  
                edition = []
                ID = []
                pic_name = []
                x1 = []
                y1 = []
                width = []
                height = []
                content = []
                part_alto_zoneID = []

                font_type = []
                font_style = []
                font_size = []
                



                flerged_package = flerge(package = p, level = 'Text')
                for unit in flerged_package:
                    if unit['@type'] == 'Text':
                        packageID.append(random_upplaga)
                        title.append(unit['title'])
                        date.append(unit['created'])
                        edition.append(unit['edition'])
                        ID.append(unit['@id'])
                        pic_name.append(unit['has_representation'][1])
                        x1.append(unit['box'][0])
                        y1.append(unit['box'][1])
                        width.append(unit['box'][2])
                        height.append(unit['box'][3])
                        content.append(unit['content'])
                        try:
                            font_type.append(str(np.unique([dd['type'] for dd in unit['font']])))
                        except:
                            font_type.append('NA')

                        try:
                            font_style.append(str(np.unique([dd['style'] for dd in unit['font']])))
                        except:
                            font_style.append('NA')


                        try:
                            font_size.append(str(np.unique([dd['size'] for dd in unit['font']])))
                        except:
                            font_size.append('NA')


                temp_dict = {"title": title,
                            "date": date,
                            "edition": edition,
                            "id": ID,
                            "pic_name": pic_name,
                            "x1" : x1,
                            "y1" : y1,
                            "width": width,
                            "height": height,
                            "content" : content,
                            'font_type': font_type,
                            'font_style': font_style,
                            'font_size': font_size}
                
                df = pd.DataFrame(temp_dict)
                
                # extract strings from content and joind, aka unlist
                df['content']= df['content'].str.join(" ") 
                
                # save file
                file_name = '/home/r3/Documents/full_data_20191118/AFTONBLADET/raw0/' + str(title[0]) + '_packageID-' + str(random_upplaga) + '.csv'
                df.to_csv(file_name, encoding = 'utf-8')
                
            else:
                print(newspaper_day)



                    


# SVENSKA DAGBLADET
import re
import random
import json
import pandas as pd
from kblab import Archive
from json import load
import csv
from kblab.utils import flerge
import numpy as np
import glob



auth = (username ,password)
a = Archive('https://datalab.kb.se', auth = auth)


random.seed(901207)


with open("/home/r3/Documents/full_data_20191118/dates_svd.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter = ',', )
    next(csv_reader, None)
    for row in csv_reader:
        newspaper_days = [row[0] + ' ' + row[1]]
        for ds in range(len(newspaper_days)):
            newspaper_day = newspaper_days[ds]
            newspaper_day = ' '.join(newspaper_day.split())
            packid = []
            for package_id in a.search({"label": newspaper_day}):
                try: 
                    p = a.get(package_id)
                    if re.search(re.compile('dark-'), str(p)):
                        packid.append(package_id)
                except:
                    print('Cannot load package')
                    
                    
            # DUE TO BUG IN API, HAVE TO MATCH TITLE WITH SEARCH STRING

            if len(packid)>=1:
                packid2 = []
                for pi in packid:
                    p = a.get(pi)
                    for fname in p:
                        if p[fname]['@type'] == 'Meta':
                            try:
                                meta_data = load(p.get_raw('meta.json'))
                                title = meta_data['title']
                                if title.replace(" ", "") == newspaper_day.replace(" ", ""):
                                    packid2.append(pi)
                            except:
                                 print('Cannot load Meta data for: ' + newspaper_day)
                                    
             # IF MORE THAN ONE EDITION, RANDOMLY PICK ONE                       
                             
                    if len(packid2)>1: 
                        random_upplaga = random.choice(packid2)
                        random_upplaga_D = 1
                    elif len(packid2) == 0:
                        random_upplaga = 'NaN'
                    else:
                        random_upplaga = packid2[0]
                        random_upplaga_D = 0
                        
                        
         
                            
            else:
                random_upplaga = 'NaN'
                    
    
            # IF EDITION EXISTS, EXTRACT STRUCTURE AND CONTENT INFO
        
            if random_upplaga != 'NaN':
                p = a.get(random_upplaga)

                packageID = []
                title = []  
                date = []  
                edition = []
                ID = []
                pic_name = []
                x1 = []
                y1 = []
                width = []
                height = []
                content = []
                part_alto_zoneID = []

                font_type = []
                font_style = []
                font_size = []
                



                flerged_package = flerge(package = p, level = 'Text')
                for unit in flerged_package:
                    if unit['@type'] == 'Text':
                        packageID.append(random_upplaga)
                        title.append(unit['title'])
                        date.append(unit['created'])
                        edition.append(unit['edition'])
                        ID.append(unit['@id'])
                        pic_name.append(unit['has_representation'][1])
                        x1.append(unit['box'][0])
                        y1.append(unit['box'][1])
                        width.append(unit['box'][2])
                        height.append(unit['box'][3])
                        content.append(unit['content'])
                        try:
                            font_type.append(str(np.unique([dd['type'] for dd in unit['font']])))
                        except:
                            font_type.append('NA')

                        try:
                            font_style.append(str(np.unique([dd['style'] for dd in unit['font']])))
                        except:
                            font_style.append('NA')


                        try:
                            font_size.append(str(np.unique([dd['size'] for dd in unit['font']])))
                        except:
                            font_size.append('NA')


                temp_dict = {"title": title,
                            "date": date,
                            "edition": edition,
                            "id": ID,
                            "pic_name": pic_name,
                            "x1" : x1,
                            "y1" : y1,
                            "width": width,
                            "height": height,
                            "content" : content,
                            'font_type': font_type,
                            'font_style': font_style,
                            'font_size': font_size}
                
                df = pd.DataFrame(temp_dict)
                
                # extract strings from content and joind, aka unlist
                df['content']= df['content'].str.join(" ") 
                
                # save file
                file_name = '/home/r3/Documents/full_data_20191118/SVENSKA DAGBLADET/raw0/' + str(title[0]) + '_packageID-' + str(random_upplaga) + '.csv'
                df.to_csv(file_name, encoding = 'utf-8')
                
            else:
                print(newspaper_day)


# EXPRESSEN
import re
import random
import json
import pandas as pd
from kblab import Archive
from json import load
import csv
from kblab.utils import flerge
import numpy as np
import glob

auth = (username ,password)
a = Archive('https://datalab.kb.se', auth = auth)


random.seed(901207)
with open("/home/r3/Documents/full_data_20191118/dates_exp.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter = ',', )
    next(csv_reader, None)
    for row in csv_reader:
        newspaper_days = [row[0] + ' ' + row[1]]
        for ds in range(len(newspaper_days)):
            newspaper_day = newspaper_days[ds]
            newspaper_day = ' '.join(newspaper_day.split())
            packid = []
            for package_id in a.search({"label": newspaper_day}):
                try: 
                    p = a.get(package_id)
                    if re.search(re.compile('dark-'), str(p)):
                        packid.append(package_id)
                except:
                    print('Cannot load package')
                    
                    
            # DUE TO BUG IN API, HAVE TO MATCH TITLE WITH SEARCH STRING

            if len(packid)>=1:
                packid2 = []
                for pi in packid:
                    p = a.get(pi)
                    for fname in p:
                        if p[fname]['@type'] == 'Meta':
                            try:
                                meta_data = load(p.get_raw('meta.json'))
                                title = meta_data['title']
                                if title.replace(" ", "") == newspaper_day.replace(" ", ""):
                                    packid2.append(pi)
                            except:
                                 print('Cannot load Meta data for: ' + newspaper_day)
                                    
             # IF MORE THAN ONE EDITION, RANDOMLY PICK ONE                       
                             
                    if len(packid2)>1: 
                        random_upplaga = random.choice(packid2)
                        random_upplaga_D = 1
                    elif len(packid2) == 0:
                        random_upplaga = 'NaN'
                    else:
                        random_upplaga = packid2[0]
                        random_upplaga_D = 0
         
                            
            else:
                random_upplaga = 'NaN'
                    
    
            # IF EDITION EXISTS, EXTRACT STRUCTURE AND CONTENT INFO
        
            if random_upplaga != 'NaN':
                p = a.get(random_upplaga)

                packageID = []
                title = []  
                date = []  
                edition = []
                ID = []
                pic_name = []
                x1 = []
                y1 = []
                width = []
                height = []
                content = []
                part_alto_zoneID = []

                font_type = []
                font_style = []
                font_size = []
                
                flerged_package = flerge(package = p, level = 'Text')
                for unit in flerged_package:
                    if unit['@type'] == 'Text':
                        packageID.append(random_upplaga)
                        title.append(unit['title'])
                        date.append(unit['created'])
                        edition.append(unit['edition'])
                        ID.append(unit['@id'])
                        pic_name.append(unit['has_representation'][1])
                        x1.append(unit['box'][0])
                        y1.append(unit['box'][1])
                        width.append(unit['box'][2])
                        height.append(unit['box'][3])
                        content.append(unit['content'])
                        try:
                            font_type.append(str(np.unique([dd['type'] for dd in unit['font']])))
                        except:
                            font_type.append('NA')

                        try:
                            font_style.append(str(np.unique([dd['style'] for dd in unit['font']])))
                        except:
                            font_style.append('NA')


                        try:
                            font_size.append(str(np.unique([dd['size'] for dd in unit['font']])))
                        except:
                            font_size.append('NA')


                temp_dict = {"title": title,
                            "date": date,
                            "edition": edition,
                            "id": ID,
                            "pic_name": pic_name,
                            "x1" : x1,
                            "y1" : y1,
                            "width": width,
                            "height": height,
                            "content" : content,
                            'font_type': font_type,
                            'font_style': font_style,
                            'font_size': font_size}
                
                df = pd.DataFrame(temp_dict)
                
                # extract strings from content and joind, aka unlist
                df['content']= df['content'].str.join(" ") 
                
                # save file
                file_name = '/home/r3/Documents/full_data_20191118/EXPRESSEN/raw0/' + str(title[0]) + '_packageID-' + str(random_upplaga) + '.csv'
                df.to_csv(file_name, encoding = 'utf-8')
                
            else:
                print(newspaper_day)


# DAGENS NYHETER

import re
import random
import json
import pandas as pd
from kblab import Archive
from json import load
import csv
from kblab.utils import flerge
import numpy as np
import glob


auth = (username ,password)
a = Archive('https://datalab.kb.se', auth = auth)
random.seed(901207)

with open("/home/r3/Documents/full_data_20191118/dates_dn.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter = ',', )
    next(csv_reader, None)
    for row in csv_reader:
        newspaper_days = [row[0] + ' ' + row[1]]
        for ds in range(len(newspaper_days)):
            newspaper_day = newspaper_days[ds]
            newspaper_day = ' '.join(newspaper_day.split())
            packid = []
            for package_id in a.search({"label": newspaper_day}):
                try: 
                    p = a.get(package_id)
                    if re.search(re.compile('dark-'), str(p)):
                        packid.append(package_id)
                except:
                    print('Cannot load package')
                    
                    
            # DUE TO BUG IN API, HAVE TO MATCH TITLE WITH SEARCH STRING

            if len(packid)>=1:
                packid2 = []
                for pi in packid:
                    p = a.get(pi)
                    for fname in p:
                        if p[fname]['@type'] == 'Meta':
                            try:
                                meta_data = load(p.get_raw('meta.json'))
                                title = meta_data['title']
                                if title.replace(" ", "") == newspaper_day.replace(" ", ""):
                                    packid2.append(pi)
                            except:
                                 print('Cannot load Meta data for: ' + newspaper_day)
                                    
             # IF MORE THAN ONE EDITION, RANDOMLY PICK ONE                       
                             
                    if len(packid2)>1: 
                        random_upplaga = random.choice(packid2)
                        random_upplaga_D = 1
                    elif len(packid2) == 0:
                        random_upplaga = 'NaN'
                    else:
                        random_upplaga = packid2[0]
                        random_upplaga_D = 0
        
            else:
                random_upplaga = 'NaN'
                    
    
            # IF EDITION EXISTS, EXTRACT STRUCTURE AND CONTENT INFO
        
            if random_upplaga != 'NaN':
                p = a.get(random_upplaga)

                packageID = []
                title = []  
                date = []  
                edition = []
                ID = []
                pic_name = []
                x1 = []
                y1 = []
                width = []
                height = []
                content = []
                part_alto_zoneID = []

                font_type = []
                font_style = []
                font_size = []
                

                flerged_package = flerge(package = p, level = 'Text')
                for unit in flerged_package:
                    if unit['@type'] == 'Text':
                        packageID.append(random_upplaga)
                        title.append(unit['title'])
                        date.append(unit['created'])
                        edition.append(unit['edition'])
                        ID.append(unit['@id'])
                        pic_name.append(unit['has_representation'][1])
                        x1.append(unit['box'][0])
                        y1.append(unit['box'][1])
                        width.append(unit['box'][2])
                        height.append(unit['box'][3])
                        content.append(unit['content'])
                        try:
                            font_type.append(str(np.unique([dd['type'] for dd in unit['font']])))
                        except:
                            font_type.append('NA')

                        try:
                            font_style.append(str(np.unique([dd['style'] for dd in unit['font']])))
                        except:
                            font_style.append('NA')


                        try:
                            font_size.append(str(np.unique([dd['size'] for dd in unit['font']])))
                        except:
                            font_size.append('NA')


                temp_dict = {"title": title,
                            "date": date,
                            "edition": edition,
                            "id": ID,
                            "pic_name": pic_name,
                            "x1" : x1,
                            "y1" : y1,
                            "width": width,
                            "height": height,
                            "content" : content,
                            'font_type': font_type,
                            'font_style': font_style,
                            'font_size': font_size}
                
                df = pd.DataFrame(temp_dict)
                
                # extract strings from content and joind, aka unlist
                df['content']= df['content'].str.join(" ") 
                
                # save file
                file_name = '/home/r3/Documents/full_data_20191118/DAGENS NYHETER/raw0/' + str(title[0]) + '_packageID-' + str(random_upplaga) + '.csv'
                df.to_csv(file_name, encoding = 'utf-8')
                
            else:
                print(newspaper_day)

