def count_file_lines(json_file=""):
    """
    # TODO 
    Read lines before using multi-threads to do data importing I/O jobs
    """
    num_lines = 0

    print("Count file lines ...")

    with open(json_file, encoding="utf8") as f:
        for _ in f:
            num_lines += 1
    return num_lines
