from typing import List
import itertools
from pprint import pprint

constants = ['4', '8', '12', '24', '48']
# indicators = ['open', 'close', 'high', 'low', 'volume', 'amount', 'trade_num', 'active_volume', 'active_amount']
indicators = ['open']
op_params = [
    {
        'op_name': 'mcorr',
        'params': ['I', 'I', 'C']
    }
]

class Op:
    def __init__(self, op_name: str, params: List[str] = [], depth: int = 20) -> None:
        '''
            op_name: 算子名称
            params: 算子参数
            depth: 当depth为0的时候, 结束
            
            如果是常量的话，不会进来
        '''
        self.op_name = op_name
        self.params = params
        self.depth = depth
        self.args: List[Op] = []
        if depth == 0:
            return
        for param in params:
            if param == 'I':
                args = []
                for indicator in indicators:
                    args.extend(Op(indicator).output())
                for _op in op_params:
                    args.extend(Op(_op['op_name'], _op['params'], depth-1).output())
            if param == 'C':
                args = []
                for constant in constants:
                    args.extend(Op(constant).output())
            self.args.append([arg for arg in args if arg != ''])
    
    def output(self) -> List[str]:
        if self.depth == 0:
            return []
            
        cartesian_product = itertools.product(*self.args)

        # 将生成的笛卡尔积转换为逗号分隔的字符串
        tot = [', '.join(map(str, combination)) for combination in cartesian_product]
        if len(tot) == 1 and tot[0] == '':
            # 叶子结点
            return [self.op_name]
        
        return [f"{self.op_name}({item})" for item in tot]
    
op = Op('1 * ', ['I'], 3)
pprint(op.output())
    
