# 9313 Spatial and Textual Similarity Join 大规模文本相似性分析

### 项目介绍：
开发一个高效的相似性检测系统，用于处理大量的文本数据点，寻找相似的文本数据，筛选出满足距离和相似度阈值的点对。可以应用于推荐系统，猜你喜欢等场景。

### 项目组成：
- project3.py (项目源代码)
- 其他测试文件
 
### 测试项目指令：
1.配置hdfs环境  

2.运行测试指令  

` spark-submit project3.py input output d s`  

参数：  
- input 输入文件  
- output 输出文件，可改为hdfs，例如file:///  
- d 距离  
- s 相似度阈值  
