sc.setLogLevel("ERROR")

# square lambda function
square = lambda s: s**2
square(4)
(lambda s: s**2)(4) # square anonymous lambda function
# creates a RDD and maps to square function
sc.parallelize([x for x in range(10)]).map(square).collect()
import random
# lambda function that takes an arbritray number or strings and returs a string
# of comma separeated values
string_csv = lambda *args : ','.join(args)
string_csv('a', 'b', 'c')
string_csv(*[str(a) for a in range(10)])
# lambda function that returns a boolean
check_boolean = lambda  x: x > 20
check_boolean(1)
check_boolean(100)
# filter a sequence of numbers using a lambda function
sc.parallelize([x for x in range(30)]).filter(check_boolean).collect()


def fibonacci(strategy, n):
    """
    Computes fibonacci using different strategies
    """
    def classic_fb(n):
        """
        Classic recursion approach
        """
        if n == 0: return 0
        elif n == 1: return 1
        else: return classic_fb(n - 1) + classic_fb(n - 2)
    def binet_fb(n):
        """
        Binet's Fibonacci Number Formula
        http://mathworld.wolfram.com/BinetsFibonacciNumberFormula.html
        """
        import math
        return (
            (1 + math.sqrt(5)) ** n -
                ( 1 - math.sqrt(5)) ** n) / (2**n*math.sqrt(5))
    strategy_dict = {'classic': classic_fb, 'binet': binet_fb}
    return strategy_dict[strategy](n)


def benchmark_fibonacci():
    from datetime import datetime
    strategies = ('classic', 'binet')
    numbers = (10, 20, 30)
    for st in strategies:
        init = datetime.utcnow()
        for n in numbers:
            r = fibonacci(st, n)
            total_time = datetime.utcnow() - init
            print(
                'Strategy: {} result: {} n: {} execution time: {}'.format(
                    st, r, n, total_time
                ))

