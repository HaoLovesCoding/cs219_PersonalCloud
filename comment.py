import pickle

def save_obj(obj, name):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def load_obj(name):
    with open(name + '.pkl', 'rb') as f:
        return pickle.load(f)


def comment_count():
    result = {}

    while True:
        comment = raw_input("Please enter a comment:\n")
        if comment == "quit":
            break
        words = comment.split()

        old_key = False
        for word in words:
            if word in result.keys():
                result[word] += [comment]
                old_key = True


        if not old_key:
            for idx, word in enumerate(words):
                print "{}:{}".format(idx,word),

            key = raw_input("Please choose a key word:\n")
            if key == "quit":
                break
            keyword = words[int(key)]
            result[keyword] = result.get(keyword,[]) + [comment]



    if len(result) == 0:
        return

    save_obj(result,"comment")
    print "dictionary saved"




def comment_load():
    result = load_obj("comment")

    total = 0
    for entry in result:
        total += len(result[entry])

    for entry in result:
        print "keyword: {}, count: {}, percentage: {:0.2f}".format(entry, len(result[entry]), float(len(result[entry]))/total)


    while True:
        to_print = raw_input("Input a keyword to print:\n")

        if to_print in result:
            print result[to_print]

        elif to_print == "quit":
            break

        else:
            print "Wrong input"
            continue


if __name__ == "__main__":
    try:
        comment_load()

    except:
        comment_count()
        comment_load()
