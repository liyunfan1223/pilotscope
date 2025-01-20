import subprocess
import os
import random

repeat = 10

def main():
    os.chdir("./qgen")
    output = ""
    for i in range(1, 23):
        if i == 15:
            continue
        for _ in range(repeat):
            # 构建命令
            command = ["./qgen", "-s", "0.1", str(i), "-r", f"{random.randint(0, 100000000)}"]
            # 执行命令
            result = subprocess.run(command, capture_output=True, text=True)
            # 检查命令是否执行成功
            if result.returncode == 0:
                lines = result.stdout.splitlines()
                if len(lines) > 1:
                    useful_output = "\n".join(lines[1:])
                    output += useful_output.replace("\n", " ").lstrip()
                else:
                    output += ""
                output += '\n'
            else:
                print(f"Command failed with error: {result.stderr}")
    # 将最终结果保存到文件中
    os.chdir("..")
    print("Save output to file: output.txt")
    with open("output.txt", "w") as f:
        f.write(output)

if __name__ == "__main__":
    main()