from scipy import sparse
import numpy as np

# 0 이 아닌 데이터 추출
dense2 = np.array([[0,0,1,0,0,5],
             [1,4,0,3,2,5],
             [0,6,0,3,0,0],
             [2,0,0,0,0,0],
             [0,0,0,7,0,8],
             [1,0,0,0,0,0]])

# 0 이 아닌 데이터 추출
data2 = np.array([1, 5, 1, 4, 3, 2, 5, 6, 3, 2, 7, 8, 1])
# 행 위치와 열 위치를 각각 array로 생성 
row_pos = np.array([0, 0, 1, 1, 1, 1, 1, 2, 2, 3, 4, 4, 5])
col_pos = np.array([2, 5, 0, 1, 3, 4, 5, 1, 3, 0, 3, 5, 0])

## 1. COO희소행렬 생성  
sparse_coo = sparse.coo_matrix((data2, (row_pos,col_pos)))
#print(type(sparse_coo)) # scipy.sparse.coo.coo_matrix
#print(sparse_coo) 
#print(sparse_coo.toarray()) 

## 2.CRS희소행렬 생성
# CSR희소행렬의 행위치 고유값
row_pos_ind = np.array([0, 2, 7, 9, 10, 12, 13])

# CSR희소행렬 생성
sparse_csr = sparse.csr_matrix((data2, col_pos, row_pos_ind))
#print(type(sparse_csr)) # scipy.sparse.csr.csr_matrix
#print(sparse_csr) 
#print(sparse_csr.toarray()) 

## 3.일반적인 방법 : 바로밀접행렬을 파라미터로 넣음
coo = sparse.coo_matrix(dense2)
csr = sparse.csr_matrix(dense2)
print(coo) # type : scipy.sparse.coo.coo_matrix
print(csr) # type : scipy.sparse.csr.csr_matrix