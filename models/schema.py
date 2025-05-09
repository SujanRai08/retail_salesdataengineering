from sqlalchemy import Column,String,Integer,Float,Date,ForeignKey
from sqlalchemy.orm import relationship
from config.database import Base

class DimCustimer(Base):
    """dimension table for customer information"""
    __tablename__ = "dim_customer"

    customer_id = Column(String, primary_key=True)
    customer_name = Column(String,nullable=True)
    segment = Column(String)
    country = Column(String)
    city = Column(String)
    state = Column(String)
    postal_code = Column(String)
    region = Column(String)

    sales = relationship('FactSales',back_populates="customer")

    def __repr__(self):
        return f"<Customer {self.customer_name}>"


class DimProduct(Base):
    """Dimension table for Product information"""
    __tablename__ = "dim_product"
    
    product_id = Column(String, primary_key=True)
    category = Column(String)
    sub_category = Column(String)
    product_name = Column(String, nullable=False)
    
    # Relationship with fact table
    sales = relationship("FactSales", back_populates="product")
    
    def __repr__(self):
        return f"<Product {self.product_name}>"
        


class DimDate(Base):
    """Dimension table for Date information"""
    __tablename__ = "dim_date"
    
    date_id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, unique=True, nullable=False)
    day = Column(Integer)
    month = Column(Integer)
    year = Column(Integer)
    quarter = Column(Integer)
    day_of_week = Column(Integer)
    is_weekend = Column(Integer)  # 0 for weekday, 1 for weekend
    
    # Relationship with fact tables
    order_dates = relationship("FactSales", 
                            foreign_keys="FactSales.order_date_id",
                            back_populates="order_date")
    ship_dates = relationship("FactSales", 
                           foreign_keys="FactSales.ship_date_id",
                           back_populates="ship_date")
    
    def __repr__(self):
        return f"<Date {self.date}>"


class FactSales(Base):
    """Fact table for Sales information"""
    __tablename__ = "fact_sales"
    
    row_id = Column(Integer, primary_key=True)
    order_id = Column(String, nullable=False)
    # Foreign keys to dimension tables
    customer_id = Column(String, ForeignKey("dim_customer.customer_id"))
    product_id = Column(String, ForeignKey("dim_product.product_id"))
    order_date_id = Column(Integer, ForeignKey("dim_date.date_id"))
    ship_date_id = Column(Integer, ForeignKey("dim_date.date_id"))
    
    # Additional attributes
    ship_mode = Column(String)
    sales = Column(Float)
    quantity = Column(Integer)
    discount = Column(Float)
    profit = Column(Float)
    profit_margin = Column(Float)
    
    # Relationships with dimension tables
    customer = relationship("DimCustomer", back_populates="sales")
    product = relationship("DimProduct", back_populates="sales")
    order_date = relationship("DimDate", foreign_keys=[order_date_id], back_populates="order_dates")
    ship_date = relationship("DimDate", foreign_keys=[ship_date_id], back_populates="ship_dates")
    
    def __repr__(self):
        return f"<Sale {self.order_id}>"

