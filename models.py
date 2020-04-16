# coding:utf-8
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.sqltypes import Integer,String
from sqlalchemy.sql.schema import Column
Base=declarative_base()


class StockBasicModel(Base):
    """
    股票列表
    接口：stock_basic
    描述：获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
    """
    ts_code=Column(String(10),primary_key=True)
    symbol=Column(String(50),nullable=False)
    name=Column(String(50),nullable=False)

    __tablename__ = 'stock_basic'


class StockAttrModel(Base):
    """
    股票下载控制属性
    """
    id = Column(Integer(),primary_key=True)
    item = Column(String(35),nullable=True)
    value = Column(String(500),nullable=True)

    __tablename__ = 'stock_attr'



# from settings import session
#
# #----------第三部分：进行数据操作--------
#
# #提交新数据
# # session.add(StockBasicModel(chr="例子",start=200,end=400))#只能加一条数据
# # session.add_all([StockBasicModel(chr="例子12",start=200,end=400),Enhancer(chr="例子12",start=200,end=400)])
# # # 使用add_all可以一次传入多条数据，以列表的形式。
# # session.commit()#提交数据
# # rs = session.query(StockBasicModel).filter(StockBasicModel.ts_code=="000001.SZ").first()
# rs = session.query(StockBasicModel).all()
# for r in rs:
#     print r.ts_code, r.symbol, r.name