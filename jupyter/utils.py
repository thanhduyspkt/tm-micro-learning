import uuid

def main():
    sort_dict_on_keys()
    generate_guid()


def sort_dict_on_keys():
    my_dict = {'b': 2, 'a': 1, 'd': 4, 'c': 3}
    sorted_dict = dict(sorted(my_dict.items()))
    print(sorted_dict)


def generate_guid():
    # Generate a new GUID
    guid = uuid.uuid4()
    # Print the GUID    
    print(guid)  

if __name__ == '__main__':
    main()
