import json
import stackx as sx

def main():
    #Load config file
    with open('config.json') as json_config_file:
        config = json.load(json_config_file)
    print(config)

    #Connect to database
    sxdb = sx.Connection(config=config["mysql"])

    a = sx.Archive7z("/Users/ardeego/repos/stackx/tests/data/test.stackexchange.com.7z")

    #Extract file to pipe (use cat on console)
    print(a.list_files())
    name = a.extract("Badges.xml", "./", pipe=True)
    a.join()

    #Extract all files
    a.extract_multiple("./")


if __name__ == '__main__':
    main()