from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from .config import *
from com.main1.pythondepmanagement_1.udfs.UDFs import *

def Script_5(spark: SparkSession, in0: DataFrame) -> DataFrame:
    from scipy.special import cbrt
    cb = cbrt([27, 64])
    print(cb)
    # import torch
    # import math
    # dtype = torch.float
    # device = torch.device("cpu")
    # x = torch.linspace(-math.pi, math.pi, 2000, device=device, dtype=dtype)
    # y = torch.sin(x)
    # a = torch.randn((), device=device, dtype=dtype)
    # print(a)
    import theano
    import theano.tensor as T
    # Define symbolic variables
    x = T.dmatrix('x')
    y = T.dmatrix('y')
    # Define the operation
    z = T.dot(x, y)
    # Create a function to execute the operation
    matrix_multiply = theano.function(inputs = [x, y], outputs = z)
    # Define sample matrices
    matrix1 = [[1, 2], [3, 4]]
    matrix2 = [[5, 6], [7, 8]]
    # Perform matrix multiplication
    result = matrix_multiply(matrix1, matrix2)
    print("Result of matrix multiplication:")
    print(result)
    out0 = in0

    return out0
