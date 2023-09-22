import pandas as pd
import uuid
import random
from faker import Faker
import datetime

# https://towardsdatascience.com/build-a-your-own-custom-dataset-using-python-9296540a0178 source

# FaceInPage
num_users = 200000

features = [
    "ID",
    "Name",
    "Nationality",
    "CountryCode",
    "Hobby"
]
face_in_page = pd.DataFrame(columns=features)

# Associates
relations = 20000000

associates_features = [
    'FriendRel',
    'PersonA_ID',
    'PersonB_ID',
    'DateOfFriendship',
    'Desc'
]


associates = pd.DataFrame(columns=associates_features)

accesses = 10000000

access_features = [
    'AccessID',
    'ByWho',
    'WhatPage',
    'TypeOfAccess',
    'AccessTime'
]

access_logs = pd.DataFrame(columns=access_features)


faker = Faker()


def FaceInPage():
    # generating ids
    face_in_page['ID'] = [(i + 1) for i in range(num_users)]

    # generating names
    face_in_page['Name'] = [faker.name() for i in range(num_users)]

    # generating nationality
    nationalities = pd.read_csv("CH_Nationality_List_20171130_v1.csv")
    nationalities_array = nationalities.to_numpy(dtype=str)
    face_in_page['Nationality'] = [nationalities_array[random.randint(0, 224)][0] for i in range(num_users)]

    # generating country code
    face_in_page['CountryCode'] = [random.randint(1, 50) for i in range(num_users)]

    # generating hobbies
    hobbies = pd.read_csv("hobbylist.csv", usecols=["Hobby-name"])
    hobbies_array = hobbies.to_numpy(dtype=str)
    face_in_page['Hobby'] = [hobbies_array[random.randint(0, len(hobbies_array)-1)][0] for i in range(num_users)]

    face_in_page.to_csv("faceInPage.csv", index=False)

relMap = {}
alist = []
blist = []


def Associates():
    # Generate FriendRel
    associates['FriendRel'] = [(i + 1) for i in range(relations)]

    # Generate Person A ID
    # Generate Person B ID
    for i in range(relations):
        (check(random.randint(1, len(face_in_page)), random.randint(1, len(face_in_page))))

    associates['PersonA_ID'] = alist
    associates['PersonB_ID'] = blist

    # Generate DateOfFriendship
    # yymmdd (getting rid of the hyphen and first two digits of the year, needs to fit in between 1 and 1,000,000)
    associates['DateOfFriendship'] = [faker.date().replace("-", "")[2::] for i in range(relations)]

    # Generating Desc
    desc_list = ['Friend', 'College Friend', 'Family', 'Relative', 'Online Friend', 'Partner', 'Other']

    associates['Desc'] = [desc_list[random.randint(0, len(desc_list) - 1)] for i in range(relations)]
    associates.to_csv('associates.csv', index=False)
    # print(associates)


# Method to help check for relations that already exist, if so then regenerate both
def check(currentA, currentB):
    # print(currentA)
    if relMap.get(currentA) == currentB or relMap.get(currentA) == currentB:
        new_A = random.randint(1, len(face_in_page))
        new_B = random.randint(1, len(face_in_page))
        check(new_A, new_B)
    else:
        relMap[currentB] = currentA
        relMap[currentA] = currentB
        alist.append(currentA)
        blist.append(currentB)


def check_dups(bywho, whatpage):
    if bywho == whatpage:
        return True
    return False


by_who_list = []
what_page_list = []
def Access_logs():
    # AccessID: unique sequential int from 1 to 10,000,000
    access_logs['AccessID'] = [(i + 1) for i in range(accesses)]

    #  ByWho: references the ID of the person who has accessed the FaceInPage
    # access_logs['ByWho'] = [random.randint(0, len(face_in_page) - 1)

    for i in range(accesses):
        bywho = random.randint(1, len(face_in_page))
        whatpage = random.randint(1, len(face_in_page))

        if (check_dups(bywho, whatpage)):
            whatpage = random.randint(1, len(face_in_page))

        by_who_list.append(bywho)
        what_page_list.append(whatpage)

    # WhatPage: references the ID of the page that was accessed

    access_logs['ByWho'] = by_who_list
    access_logs['WhatPage'] = what_page_list

    # Generate TypeOfAccess
    type_list = ['just viewed', 'left a note', 'added a friendship', 'reported', 'left a like']
    access_logs['TypeOfAccess'] = [type_list[random.randint(0, len(type_list) - 1)] for i in range(accesses)]

    # AccessTime: random number between 1 and 1,000,000 (or epoch time
    access_logs['AccessTime'] = [(random.randint(1, 1000000)) for i in range(accesses)]

    access_logs.to_csv('accessLogs.csv', index=False)
    # print(access_logs)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    FaceInPage()
    Associates()
    Access_logs()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
