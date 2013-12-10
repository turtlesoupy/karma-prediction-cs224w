import sqlite3
import os
import logging
from lxml import etree
import argparse

ANATOMY = {
    'badges': {
        'Id':'INTEGER',
        'UserId':'INTEGER',
        'Name':'TEXT',
        'Date':'DATETIME',
    },
    'comments': {
        'Id':'INTEGER',
        'PostId':'INTEGER',
        'Score':'INTEGER',
        'Text':'TEXT',
        'CreationDate':'DATETIME',
        'UserDisplayName':'TEXT',
        'UserId':'INTEGER',
    },
    'posts': {
        'Id':'INTEGER',
        'PostTypeId':'INTEGER', # 1: Question, 2: Answer
        'ParentID':'INTEGER', # (only present if PostTypeId is 2)
        'AcceptedAnswerId':'INTEGER', # (only present if PostTypeId is 1)
        'CreationDate':'DATETIME',
        'Score':'INTEGER',
        'ViewCount':'INTEGER',
        'Body':'TEXT',
        'OwnerUserId':'INTEGER', # (present only if user has not been deleted)
        'OwnerDisplayName':'TEXT',
        'LastEditorUserId':'INTEGER',
        'LastEditorDisplayName':'TEXT',
        'LastEditDate':'DATETIME',
        'LastActivityDate':'DATETIME',
        'CommunityOwnedDate':'DATETIME', #(present only if post is community wikied)
        'Title':'TEXT',
        'Tags':'TEXT',
        'AnswerCount':'INTEGER',
        'CommentCount':'INTEGER',
        'FavoriteCount':'INTEGER',
        'ClosedDate':'DATETIME',
    },
    'votes': {
        'Id':'INTEGER',
        'BountyAmount':'INTEGER',
        'PostId':'INTEGER',
        'UserId':'INTEGER',
        'VoteTypeId':'INTEGER',
        # -   1: AcceptedByOriginator
        # -   2: UpMod
        # -   3: DownMod
        # -   4: Offensive
        # -   5: Favorite
        # -   6: Close
        # -   7: Reopen
        # -   8: BountyStart
        # -   9: BountyClose
        # -  10: Deletion
        # -  11: Undeletion
        # -  12: Spam
        # -  13: InformModerator
        'CreationDate':'DATETIME',
    },
    'users': {
        'Id':'INTEGER',
        'Reputation':'INTEGER',
        'CreationDate':'DATETIME',
        'DisplayName':'TEXT',
        'LastAccessDate':'DATETIME',
        'WebsiteUrl':'TEXT',
        'Location':'TEXT',
        'Age':'INTEGER',
        'AboutMe':'TEXT',
        'Views':'INTEGER',
        'UpVotes':'INTEGER',
        'DownVotes':'INTEGER',
        'ProfileImageUrl':'TEXT',
        'EmailHash':'TEXT',
    },
}

CREATE_QUERY = 'CREATE TABLE IF NOT EXISTS [{table}]({fields})'
INSERT_QUERY = 'INSERT INTO {table} ({columns}) VALUES ({values})'

def dump_tables(table_names, anatomy, xml_path, dump_path, dump_database_name, log_filename='dump.log'):
    logging.basicConfig(filename=os.path.join(dump_path, log_filename), level=logging.INFO)
    db = sqlite3.connect(os.path.join(dump_path, dump_database_name))

    for table_name in table_names:
        print "Opening {0}.xml".format(table_name)
        with open(os.path.join(xml_path, table_name + '.xml')) as xml_file:
            tree = etree.iterparse(xml_file)

            sql_create = CREATE_QUERY.format(
                    table=table_name,
                    fields=", ".join(['{0} {1}'.format(name, type) for name, type in anatomy[table_name].items()]))
            print 'Creating table {0}'.format(table_name)

            try:
                logging.info(sql_create)
                db.execute(sql_create)
            except Exception, e:
                logging.warning(e)

            for _, row in etree.iterparse(xml_file, tag="row"):
                try:
                    logging.debug(row.attrib.keys())
                    db.execute(INSERT_QUERY.format(
                        table=table_name,
                        columns=', '.join(row.attrib.keys()),
                        values=('?, ' * len(row.attrib.keys()))[:-2]),
                        row.attrib.values())
                    print ".",
                except Exception, e:
                    logging.warning(e)
                    print "x",
                finally:
                    row.clear()
            print
            db.commit()
            del(tree)
    db.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Extract data from StackOverflow")
    parser.add_argument("dump_path", help="the path to create the database and extrqact to")
    parser.add_argument("xml_path", help="the path containing the XML files")
    parser.add_argument("--logfile", help="log filename")
    args = parser.parse_args()
    if args.logfile:
        dump_tables(ANATOMY.keys(), ANATOMY, args.xml_path, args.dump_path, "stackoverflow.db", args.logfile)
    else:
        dump_tables(ANATOMY.keys(), ANATOMY, args.xml_path, args.dump_path, "stackoverflow.db")
