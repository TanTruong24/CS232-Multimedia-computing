from lxml import etree
from datetime import datetime
import csv
import codecs
from sqlalchemy import null
import ujson
import re
import mysql.connector
from mysql.connector import Error
import pandas as pd

# =============== Create-connect MySQL =====================


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


# Creating a New Database
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

# Connecting to the Database


def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


# Creating a Query Execution Function
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")


def create_table():
    # Creating Tables
    article_table = """
    CREATE TABLE article (
        address VARCHAR(300),
        author VARCHAR(400) NOT NULL,
        booktitle VARCHAR(400),
        cdrom VARCHAR(500),
        chapter VARCHAR(300),
        cite VARCHAR(300),
        crossref VARCHAR(300),
        editor VARCHAR(300),
        ee VARCHAR(500),
        isbn VARCHAR(300),
        journal VARCHAR(300),
        month VARCHAR(50),
        note VARCHAR(300),
        number VARCHAR(300),
        publisher VARCHAR(500),
        school VARCHAR(300),
        series VARCHAR(300),
        title VARCHAR(300) PRIMARY KEY,
        url VARCHAR(700),
        volume VARCHAR(300),
        year INT NOT NULL,
        pages INT
    );
    """

    inproceedings_table = """
    CREATE TABLE inproceedings (
        title VARCHAR(300) PRIMARY KEY,
        author VARCHAR(300) NOT NULL,
        booktitle VARCHAR(300),
        year INT NOT NULL,
        pages INT
    );
    """

    proceedings_table = """
    CREATE TABLE proceedings (
        title VARCHAR(300) PRIMARY KEY,
        editor VARCHAR(300) NOT NULL,
        booktitle VARCHAR(300),
        series VARCHAR(100),
        publisher VARCHAR(300) NOT NULL,
        year INT NOT NULL
    )
    """
    book_table = """
    CREATE TABLE book (
        title VARCHAR(300) PRIMARY KEY,
        author VARCHAR(300) NOT NULL,
        publisher VARCHAR(300) NOT NULL,
        isbn VARCHAR(100),
        year INT,
        pages INT
    )
    """
    author_table = """
    CREATE TABLE author (
        author VARCHAR(100) PRIMARY KEY
    )
    """

    publication_table = """
    CREATE TABLE publication (
        title VARCHAR(300) PRIMARY KEY,
        year INT NOT NULL,
        pages INT
    );
    """

    return article_table, inproceedings_table, proceedings_table, book_table, author_table, publication_table


def insert_record(connection, record, choose_table):
    if (choose_table == 0):
        pop_author = """
        INSERT INTO author VALUES {};
        """.format(record)
        print(pop_author)
        execute_query(connection, pop_author)
    elif (choose_table == 1):
        pop_proceedings = """
        INSERT INTO proceedings VALUES {};
        """.format(record)
        execute_query(connection, pop_proceedings)
    elif (choose_table == 2):
        pop_inproceedings = """
        INSERT INTO inproceedings VALUES {};
        """.format(record)
        execute_query(connection, pop_inproceedings)
    elif (choose_table == 3):
        pop_article = """
        INSERT INTO article VALUES {};
        """.format(record)
        execute_query(connection, pop_article)
    elif (choose_table == 4):
        pop_book = """
        INSERT INTO book VALUES {};
        """.format(record)
        execute_query(connection, pop_book)
    elif (choose_table == 5):
        pop_publication = """
        INSERT INTO publication VALUES {};
        """.format(record)
        execute_query(connection, pop_publication)

# =============== Parse XML =====================


# all of the element types in dblp
all_elements = {"article", "inproceedings", "proceedings",
                "book", "incollection", "phdthesis", "mastersthesis", "www"}
# all of the feature types in dblp
all_features = {"address", "author", "booktitle", "cdrom", "chapter", "cite", "crossref", "editor", "ee", "isbn",
                "journal", "month", "note", "number", "pages", "publisher", "school", "series", "title", "url",
                "volume", "year"}


def log_msg(message):
    """Produce a log with current time"""
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message)


def context_iter(dblp_path):
    """Create a dblp data iterator of (event, element) pairs for processing"""
    return etree.iterparse(source=dblp_path, dtd_validation=True, load_dtd=True)  # required dtd


def clear_element(element):
    """Free up memory for temporary element tree after processing the element"""
    element.clear()
    while element.getprevious() is not None:
        del element.getparent()[0]


def count_pages(pages):
    """Borrowed from: https://github.com/billjh/dblp-iter-parser/blob/master/iter_parser.py
    Parse pages string and count number of pages. There might be multiple pages separated by commas.
    VALID FORMATS:
        51         -> Single number
        23-43      -> Range by two numbers
    NON-DIGITS ARE ALLOWED BUT IGNORED:
        AG83-AG120
        90210H     -> Containing alphabets
        8e:1-8e:4
        11:12-21   -> Containing colons
        P1.35      -> Containing dots
        S2/109     -> Containing slashes
        2-3&4      -> Containing ampersands and more...
    INVALID FORMATS:
        I-XXI      -> Roman numerals are not recognized
        0-         -> Incomplete range
        91A-91A-3  -> More than one dash
        f          -> No digits
    ALGORITHM:
        1) Split the string by comma evaluated each part with (2).
        2) Split the part to subparts by dash. If more than two subparts, evaluate to zero. If have two subparts,
           evaluate by (3). If have one subpart, evaluate by (4).
        3) For both subparts, convert to number by (4). If not successful in either subpart, return zero. Subtract first
           to second, if negative, return zero; else return (second - first + 1) as page count.
        4) Search for number consist of digits. Only take the last one (P17.23 -> 23). Return page count as 1 for (2)
           if find; 0 for (2) if not find. Return the number for (3) if find; -1 for (3) if not find.
    """
    cnt = 0
    for part in re.compile(r",").split(str(pages)):
        subparts = re.compile(r"-").split(part)
        if len(subparts) > 2:
            continue
        else:
            try:
                re_digits = re.compile(r"[\d]+")
                subparts = [int(re_digits.findall(sub)[-1])
                            for sub in subparts]
            except IndexError:
                continue
            cnt += 1 if len(subparts) == 1 else subparts[1] - subparts[0] + 1
    return "" if cnt == 0 else str(cnt)


def extract_feature(elem, features, include_key=False):
    """Extract the value of each feature"""
    if include_key:
        attribs = {'key': [elem.attrib['key']]}
    else:
        attribs = {}
    for feature in features:
        attribs[feature] = []
    for sub in elem:
        if sub.tag not in features:
            continue
        if sub.tag == 'title':
            text = re.sub("<.*?>", "", etree.tostring(sub).decode('utf-8')
                          ) if sub.text is None else sub.text
        elif sub.tag == 'pages':
            text = count_pages(sub.text)
        else:
            text = sub.text
        if text is not None and len(text) > 0:
            attribs[sub.tag] = attribs.get(sub.tag) + [text]
    return attribs


def parse_all(dblp_path, save_path, include_key=False):
    log_msg("PROCESS: Start parsing...")
    f = open(save_path, 'w', encoding='utf8')
    for _, elem in context_iter(dblp_path):
        if elem.tag in all_elements:
            attrib_values = extract_feature(elem, all_features, include_key)
            f.write(str(attrib_values) + '\n')
        clear_element(elem)
    f.close()
    log_msg("FINISHED...")  # load the saved results line by line using json


def parse_entity(dblp_path, type_name, connection, choose_table, save_path=None, features=None, save_to_csv=False, save_to_json=False, include_key=False):
    """Parse specific elements according to the given type name and features"""
    log_msg("PROCESS: Start parsing for {}...".format(str(type_name)))
    assert features is not None, "features must be assigned before parsing the dblp dataset"
    results = []
    attrib_count, full_entity, part_entity = {}, 0, 0
    for _, elem in context_iter(dblp_path):
        if elem.tag in type_name:
            attrib_values = extract_feature(
                elem, features, include_key)  # extract required features
            results.append(attrib_values)  # add record to results array
            for key, value in attrib_values.items():
                attrib_count[key] = attrib_count.get(key, 0) + len(value)
            cnt = sum(
                [1 if len(x) > 0 else 0 for x in list(attrib_values.values())])
            if cnt == len(features):
                full_entity += 1
            else:
                part_entity += 1
        elif elem.tag not in all_elements:
            continue
        clear_element(elem)
    if save_to_csv:
        f = open(save_path, 'w', newline='', encoding='utf8')
        writer = csv.writer(f, delimiter=',')
        writer.writerow(features)  # write title
        for record in results:
            # some features contain multiple values (e.g.: author), concatenate with `::`
            row = ['::'.join(v) for v in list(record.values())]
            writer.writerow(row)
        f.close()
    elif save_to_json:  # save to json file
        with codecs.open(save_path, mode='w', encoding='utf8', errors='ignore') as f:
            ujson.dump(results, f)
    else:
        i = 0
        if (choose_table > 1):
            for record in results:
                print(i)
                # some features contain multiple values (e.g.: author), concatenate with `::`
                row = ['::'.join(v) for v in list(record.values())]
                row[-2] = int(row[-2])
                if (len(row[-1]) != 0):
                    row[-1] = int(row[-1])
                else:
                    row[-1] = -1
                row = str(tuple(row))
                insert_record(connection, row, choose_table)
                i += 1
        elif (choose_table == 1):
            for record in results:
                print(i)
                # some features contain multiple values (e.g.: author), concatenate with `::`
                row = ['::'.join(v) for v in list(record.values())]
                row[-1] = int(row[-1])
                row = str(tuple(row))
                insert_record(connection, row, choose_table)
                i += 1
        else:
            for record in results:
                print(i)
                # some features contain multiple values (e.g.: author), concatenate with `::`
                row = ['::'.join(v) for v in list(record.values())]
                row = str(tuple(row))
                insert_record(connection, row, choose_table)
                i += 1

    return full_entity, part_entity, attrib_count


def parse_author(dblp_path, connection, save_path=None, save_to_csv=False, save_to_json=False):
    type_name = ['article', 'book', 'incollection', 'inproceedings']
    log_msg("PROCESS: Start parsing for {}...".format(str(type_name)))
    authors = set()
    for _, elem in context_iter(dblp_path):
        if elem.tag in type_name:
            authors.update(a.text for a in elem.findall('author'))
        elif elem.tag not in all_elements:
            continue
        clear_element(elem)
    if save_to_csv:
        f = open(save_path, 'w', newline='', encoding='utf8')
        writer = csv.writer(f, delimiter=',')
        writer.writerows([a] for a in sorted(authors))
        f.close()
    elif save_to_json:
        with open(save_path, 'w', encoding='utf8') as f:
            f.write('\n'.join(sorted(authors)))
    else:
        for record in sorted(authors):
            temp = "('{}')".format(record)
            insert_record(connection, temp, 0)
    log_msg("FINISHED...")


def parse_article(dblp_path, connection, save_path=None, save_to_csv=False, save_to_json=False, include_key=False):
    type_name = ['article']
    #features = ['title', 'author', 'journal', 'year', 'pages']
    #save_path = "./article.csv"
    features = ["address", "author", "booktitle", "cdrom", "chapter", "cite", "crossref", "editor", "ee", "isbn",
                "journal", "month", "note", "number", "publisher", "school", "series", "title", "url",
                "volume", "year", "pages"]
    info = parse_entity(dblp_path, type_name, connection, 3, save_path, features,
                        save_to_csv=save_to_csv, save_to_json=save_to_json, include_key=include_key)
    log_msg('Total articles found: {}, articles contain all features: {}, articles contain part of features: {}'
            .format(info[0] + info[1], info[0], info[1]))
    log_msg("Features information: {}".format(str(info[2])))


def parse_inproceedings(dblp_path, connection, save_path=None, save_to_csv=False, save_to_json=False, include_key=False):
    type_name = ["inproceedings"]
    #features = ['title', 'author', 'booktitle', 'year', 'pages']
    info = parse_entity(dblp_path, type_name, connection, 2, save_path, features,
                        save_to_csv=save_to_csv, save_to_json=save_to_json, include_key=include_key)
    log_msg('Total inproceedings found: {}, inproceedings contain all features: {}, inproceedings contain part of '
            'features: {}'.format(info[0] + info[1], info[0], info[1]))
    log_msg("Features information: {}".format(str(info[2])))


def parse_proceedings(dblp_path, connection, save_path=None, save_to_csv=False, save_to_json=False, include_key=False):
    type_name = ["proceedings"]
    features = ['title', 'editor', 'booktitle', 'series', 'publisher', 'year']
    # Other features are 'volume','isbn' and 'url'.
    info = parse_entity(dblp_path, type_name, connection, 1, save_path, features,
                        save_to_csv=save_to_csv, save_to_json=save_to_json, include_key=include_key)
    log_msg('Total proceedings found: {}, proceedings contain all features: {}, proceedings contain part of '
            'features: {}'.format(info[0] + info[1], info[0], info[1]))
    log_msg("Features information: {}".format(str(info[2])))


def parse_book(dblp_path, connection, save_path=None, save_to_csv=False, save_to_json=False, include_key=False):
    type_name = ["book"]
    features = ['title', 'author', 'publisher', 'isbn', 'year', 'pages']
    info = parse_entity(dblp_path, type_name, connection, 4, save_path, features,
                        save_to_csv=save_to_csv, save_to_json=save_to_json, include_key=include_key)
    log_msg('Total books found: {}, books contain all features: {}, books contain part of features: {}'
            .format(info[0] + info[1], info[0], info[1]))
    log_msg("Features information: {}".format(str(info[2])))


def parse_publications(dblp_path, connection, save_path=None, save_to_csv=False, save_to_json=False, include_key=False):
    type_name = ['article', 'book', 'incollection', 'inproceedings']
    features = ['title', 'year', 'pages']
    info = parse_entity(dblp_path, type_name, connection, 5, save_path, features,
                        save_to_csv=save_to_csv, save_to_json=save_to_json, include_key=include_key)
    log_msg('Total publications found: {}, publications contain all features: {}, publications contain part of '
            'features: {}'.format(info[0] + info[1], info[0], info[1]))
    log_msg("Features information: {}".format(str(info[2])))


def main():
    dblp_path = "./dblp.xml"
    """
    save_path1 = "./proceedings.csv"
    save_path2 = "./book.csv"
    save_path3 = "./publication.csv"
    save_path4 = "./author.csv"
    """
    try:
        context_iter(dblp_path)
        log_msg("LOG: Successfully loaded \"{}\".".format(dblp_path))
    except IOError:
        log_msg("ERROR: Failed to load file \"{}\". Please check your XML and DTD files.".format(
            dblp_path))
        exit()

    db_name = "papers"
    pw = "tansql24*"
    connection = create_server_connection("localhost", "root", pw)
    create_database_query = "CREATE DATABASE papers"
    create_database(connection, create_database_query)
    connection = create_db_connection(
        "localhost", "root", pw, db_name)  # Connect to the Database

    # Execute our defined query
    create_article_table, create_inproceedings_table, create_proceedings_table, create_book_table, create_author_table, create_publication_table = create_table()
    execute_query(connection, create_article_table)
    execute_query(connection, create_inproceedings_table)
    execute_query(connection, create_publication_table)
    execute_query(connection, create_author_table)
    execute_query(connection, create_book_table)
    execute_query(connection, create_proceedings_table)

    parse_article(dblp_path, connection)
    #parse_inproceedings(dblp_path, connection)
    #parse_proceedings(dblp_path, connection)
    #parse_book(dblp_path, connection)
    #parse_publication(dblp_path, connection)
    #parse_author(dblp_path, connection)


if __name__ == '__main__':
    main()
