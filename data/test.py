import pandas as pd
pd1 = pd.read_csv(r"C:\Users\29967\Desktop\选股软件\data\raw\daily_close.csv", index_col=0)
pd2 = pd.read_csv(r"C:\Users\29967\Desktop\选股软件\data\raw\daily_open.csv", index_col=0)

# 查看每个索引出现的次数
index_counts = pd2.index.value_counts()
duplicate_counts = index_counts[index_counts > 1]
print("重复索引及其出现次数:")
print(duplicate_counts)



list1 = pd1.index.tolist()
list2 = pd2.index.tolist()
# 找出 pd1 有但 pd2 没有的索引
only_in_pd1_index = set(pd1.index) - set(pd2.index)

# 找出 pd2 有但 pd1 没有的索引
only_in_pd2_index = set(pd2.index) - set(pd1.index)

# 找出共同的索引
common_index = set(pd1.index) & set(pd2.index)

print("仅在 pd1 中的索引:", only_in_pd1_index)
print("仅在 pd2 中的索引:", only_in_pd2_index)
print("共同索引:", common_index)
