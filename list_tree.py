import os

def list_files(startpath):
    print(f"專案路徑: {os.path.abspath(startpath)}\n")
    for root, dirs, files in os.walk(startpath):
        # 過濾掉不需要看到的資料夾（如 .git, __pycache__）
        dirs[:] = [d for d in dirs if d not in ['.git', '__pycache__', '.ipynb_checkpoints']]
        
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print(f'{indent}{os.path.basename(root)}/')
        sub_indent = ' ' * 4 * (level + 1)
        for f in files:
            print(f'{sub_indent}{f}')

if __name__ == "__main__":
    # 指定你的專案路徑
    project_path = r'C:\code\intraday-limitup-engine'
    list_files(project_path)