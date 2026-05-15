from sqlalchemy import Column, DateTime, Integer, Numeric, String 
from core.db import Base


class Customer(Base):
  __tablename__ = "src_customers"

  customer_id = Column(Integer, primary_key=True)
  name = Column(String)
  email = Column(String)
  country = Column(String) 
  created_at = Column(DateTime(timezone=True), index=True, nullable=False)


class Billing(Base):
  __tablename__ = "src_billing_transactions"

  transaction_id = Column(Integer, primary_key=True)
  customer_id = Column(Integer, nullable=True)
  amount = Column(Numeric(precision=10, scale=2), nullable=True) 
  currency = Column(String)
  transaction_date = Column(DateTime(timezone=True), index=True, nullable=False)


class NetworkSession(Base):
  __tablename__ = "src_network_sessions"

  session_id = Column(Integer, primary_key=True)
  customer_id = Column(Integer, nullable=True)
  data_used_mb = Column(Numeric(precision=10, scale=2), nullable=True) 
  start_time = Column(String)
  end_time = Column(String)