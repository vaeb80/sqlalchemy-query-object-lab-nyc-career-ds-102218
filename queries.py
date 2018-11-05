from sqlalchemy import create_engine, func, and_, or_
from seed import Company
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///dow_jones.db', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def return_apple():
    return session.query(Company).filter_by(company="Apple").first()

def return_disneys_industry():
    return session.query(Company).filter_by(company="Walt Disney").first().industry

def return_list_of_company_objects_ordered_alphabetically_by_symbol():
    return session.query(Company).order_by(Company.symbol).all()

def return_list_of_dicts_of_tech_company_names_and_their_EVs_ordered_by_EV_descending():
    company_list = []
    for x in session.query(Company).filter_by(industry = "Technology").order_by(Company.enterprise_value.desc()):
        company_list.append({"company":x.company, "EV": x.enterprise_value})
    return company_list

def return_list_of_consumer_products_companies_with_EV_above_225():
    company_list = []
    for x in session.query(Company).\
        filter(and_(Company.industry == "Consumer products", Company.enterprise_value > 225)).\
        order_by(Company.enterprise_value).all():
        company_list.append({"name" : x.company})
    return company_list

def return_conglomerates_and_pharmaceutical_companies():
    company_objects = session.query(Company).\
        filter(or_(Company.industry == "Conglomerate",Company.industry=="Pharmaceuticals")).all()
    company_list = []
    for x in company_objects:
        company_list.append(x.company)
    return company_list

def avg_EV_of_dow_companies():
    return session.query(func.avg(Company.enterprise_value)).first()


def return_industry_and_its_total_EV():
    return session.query(Company.industry ,func.sum(Company.enterprise_value).label("Industry Total")).\
        group_by(Company.industry).order_by(Company.industry).all()

def count_number_of_tech_companies():
    return session.query(Company).filter_by(industry = "Technology").count()
