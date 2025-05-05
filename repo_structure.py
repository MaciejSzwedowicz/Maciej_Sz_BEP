import os

def print_tree(startpath, max_depth=4):
    startpath = os.path.abspath(startpath)
    for root, dirs, files in os.walk(startpath):
        depth = root.replace(startpath, '').count(os.sep)
        if depth > max_depth:
            continue
        indent = '│   ' * depth + '├── '
        print(f"{indent}{os.path.basename(root)}/")
        subindent = '│   ' * (depth + 1)
        for f in files:
            print(f"{subindent}{f}")

# Replace '.' with the path to your local repo if needed
print_tree('.')
