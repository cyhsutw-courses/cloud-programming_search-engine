import sys
import os


def main(argv):
    root_dir = '/user/s103062512/{0}'.format(argv[0])
    script_dir = os.path.dirname(os.path.realpath(__file__))
    with open(script_dir + '/../target/create_table.sql', 'w+') as f:
        f.write("DROP TABLE pagerank;\n")
        f.write("CREATE EXTERNAL TABLE IF NOT EXISTS pagerank (docid INT, rank DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ':' STORED AS TEXTFILE location '{0}/rank';\n".format(root_dir))

        f.write("DROP TABLE invertedindex;\n")
        f.write("CREATE EXTERNAL TABLE IF NOT EXISTS invetedindex (docid INT, rank DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;' STORED AS TEXTFILE location '{0}/index';\n".format(root_dir))


if __name__ == '__main__':
    main(sys.argv)
