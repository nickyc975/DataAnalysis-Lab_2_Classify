file_name = "../data/SUSY.csv"
train_file_name = "../data/SUSY_train.csv"
test_file_name = "../data/SUSY_test.csv"

with open(file_name, "r") as input_file:
    with open(train_file_name, "w") as train_file:
        with open(test_file_name, "w") as test_file:
            i = 0
            lines = input_file.readlines()
            for line in lines:
                if i % 5 == 0:
                    test_file.write(line)
                else:
                    train_file.write(line)
                i += 1