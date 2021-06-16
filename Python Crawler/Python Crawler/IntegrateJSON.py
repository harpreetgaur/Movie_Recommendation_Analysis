import os

# read file list
player_list = os.listdir('../PlayerDB/NA_BASIC')
# total file numbers
length = len(player_list)
# counter initialized
count = 0

# write to file
with open('../PlayerDB/na_basic.json', 'w') as output_file:
    # loop files
    for p in player_list:
        with open('../PlayerDB/NA_BASIC/' + p) as input_file:
            # append to na_basic.json
            output_file.write(input_file.read() + '\n')
            # counter increment
            count += 1
            # display processing percentage
            print(str(round((count/length*100), 2)) + '%')
