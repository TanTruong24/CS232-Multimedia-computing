import math

from regex import A

matrix = [[2, 3, 1, 1],
          [1, 2, 3, 1],
          [1, 1, 2, 3],
          [3, 1, 1, 2]]


def binToHexa(n):

    # convert binary to int
    num = int(n, 2)

    # convert int to hexadecimal
    hex_num = format(num, 'x')
    return(hex_num)


def multiply02(a):
    temp = "{0:08b}".format(int(a, 16))
    res = temp[1:8] + "0"

    if temp[0] == "1":
        res = hex(int(binToHexa(res), 16) ^ int("1b", 16))
        res = res.replace('0x', '')
        return res

    return str(hex(int(res, 2))).replace('0x', '')


def mixColumns(a, x):
    ele = []
    for i in range(4):
        if x[i] == 1:
            ele.append(a[i])
        elif x[i] == 2:
            ele.append(multiply02(a[i]))
        else:
            ele.append(a[i])
            ele.append(multiply02(a[i]))

    # print(ele)
    sum = ele[0]
    for i in range(1, 5):
        sum = hex(int(sum, 16) ^ int(ele[i], 16))
        sum = sum.replace('0x', '')

    print("res:", sum)


a1, a2, a3, a4 = input().split()
a = [a1, a2, a3, a4]
for i in range(4):
    mixColumns(a, matrix[i])
