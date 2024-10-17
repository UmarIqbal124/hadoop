from hdfs import InsecureClient
from collections import Counter

# HDFS client setup
client = InsecureClient('http://localhost:9870', user='hadoop')
  # hdfs_path = '/user/test_file.txt'
#output_path = './result.txt'
def word_count_from_hdfs(hdfs_path, output_path):
    # Read file from HDFS
    with client.read(hdfs_path, encoding='utf-8') as reader:
        # Initialize a Counter to count the words
        word_count = Counter(reader.read().split())

    # Write the word counts to a local file or HDFS
    with open(output_path, 'w') as output_file:
        for word, count in word_count.items():
            output_file.write(f'{word}: {count}\n')
    print(f"Word count written to {output_path}")

if __name__ == "__main__":
    # Specify the file in HDFS and output file
    hdfs_input_file = '/user/test_file.txt'  # Path on HDFS
    local_output_file = './text_analysis_output.txt'  # Output file on local system

    # Perform word count and output results
    word_count_from_hdfs(hdfs_input_file, local_output_file)
