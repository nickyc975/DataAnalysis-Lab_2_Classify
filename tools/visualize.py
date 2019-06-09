import matplotlib.pyplot as plt
import pickle

cluster_result = []
with open("../data/part-00000", "r") as result:
    lines = result.readlines()
    for line in lines:
        cluster_result.append(eval(line)[0])

# 从pickle文件读取降维结果
with open("../data/usdata.pickle", "rb") as usdata:
    data = pickle.load(usdata)
y = cluster_result[:10000]  # 这里，y表示聚类结果（一维向量，list或者numpy.array都可以
plt.scatter(data[:, 0], data[:, 1], c=y)

plt.show()