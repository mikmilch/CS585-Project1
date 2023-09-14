import pandas as pd
import uuid
import random
from faker import Faker
import datetime

# https://towardsdatascience.com/build-a-your-own-custom-dataset-using-python-9296540a0178 source
num_users = 1000
features = [
    "ID",
    "Name",
    "Nationality",
    "CountryCode",
    "Hobby"
]
face_in_page = pd.DataFrame(columns=features)

relations = 1000  # 19999999

associates_features = [
    'FriendRel',
    'PersonAID',
    'PersonBID',
    'DateOfFriendship',
    'Desc'
]

associates = pd.DataFrame(columns=associates_features)

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
    print(face_in_page['Nationality'])

    # generating country code

    # generating hobbies


relMap = {}
alist = []
blist = []


def Associates():
    # Generate FriendRel
    associates['FriendRel'] = [(i + 1) for i in range(relations)]

    # Generate Person A ID
    # Generate Person B ID
    for i in range(relations):
        currentA = random.randint(0, len(face_in_page))
        currentB = random.randint(0, len(face_in_page))
        check(currentA, currentB)

        relMap[currentB] = currentA
        relMap[currentA] = currentB
        alist.append(currentA)
        blist.append(currentB)
    associates['PersonAID'] = alist
    associates['PersonBID'] = blist

    # Generate DateOfFriendship
    # yymmdd (getting rid of the hyphen and first two digits of the year, needs to fit in between 1 and 1,000,000)
    associates['DateOfFriendship'] = [faker.date().replace("-", "")[2::] for i in range(relations)]
    print(associates)


# Method to help check for relations that already exist, if so then regenerate both
def check(currentA, currentB):
    if relMap.get(currentA) == currentB or relMap.get(currentA) == currentB:
        print("Duplicated " + str(currentA) + ", " + str(currentB))
        new_currentA = random.randint(0, len(face_in_page))
        new_currentB = random.randint(0, len(face_in_page))
        check(new_currentA, new_currentB)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    FaceInPage()
    Associates()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
