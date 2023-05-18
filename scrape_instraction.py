import pandas as pd
import json
# coding: utf-8
from sqlalchemy import ARRAY, Boolean, Column, Date, DateTime, Enum, Float, ForeignKey, Index, Integer, Numeric, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, text, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table

Base = declarative_base()
metadata = Base.metadata


class AssistanceEmployment(Base):
    __tablename__ = 'assistance_employments'

    id = Column(Integer, primary_key=True, server_default=text("nextval('service_list_id_service_seq'::regclass)"))
    name = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(precision=3), server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))


class Doctor(Base):
    __tablename__ = 'doctors'

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    _link = Column(Text)
    created_at = Column(TIMESTAMP(precision=3), nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))
    gender = Column(Text)
    npi = Column(Integer, unique=True)
    primary_specialization = Column(Text)
    years_experience = Column(Text)
    overall_patient_experience_value = Column(Integer)
    reviews_total = Column(Integer)
    reviews_last_year = Column(Integer)
    is_sole_proprietor = Column(Boolean)
    npi_update_date = Column(Date)
    certification_date = Column(Date)


class HealthFacility(Base):
    __tablename__ = 'health_facilities'

    npi = Column(Integer, nullable=False, unique=True)
    name = Column(String)
    subpart = Column(Boolean)
    enumeration_data = Column(Date)
    npi_update_date = Column(Date)
    certification_data = Column(Date)
    authorized_official_name = Column(Text)
    authorized_official_telephone = Column(String)
    authorized_official_title_or_position = Column(String)
    name_parent_organization = Column(String)
    is_subpart = Column(Boolean)
    created_at = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)
    type = Column(String)
    id = Column(Integer, primary_key=True, server_default=text("nextval('health_facilities_id_seq'::regclass)"))


class Hospital(Base):
    __tablename__ = 'hospitals'
    __table_args__ = {'comment': 'aha hospitals'}

    id = Column(Integer, primary_key=True)
    name = Column(String(250), nullable=False)
    description = Column(Text)
    _link = Column(Text)
    created_at = Column(TIMESTAMP(precision=6))
    updated_at = Column(TIMESTAMP(precision=6))
    latitude = Column(Numeric(65, 30))
    longitude = Column(Numeric(65, 30))
    naics_description = Column(String(100))
    alternative_names = Column(ARRAY(String()))
    owner = Column(String(200))
    level_trauma = Column(String(100))
    is_helipad = Column(Boolean)
    status = Column(Enum('open', 'closed', name='status'))
    specialization = Column(Enum('general acute care', 'psychiatric', 'military', 'special', 'critical access', 'rehabilitation', 'children', 'long term care', 'chronic disease', 'women', name='specialization'))
    beds = Column(Integer)
    aha_id = Column(Integer, unique=True)
    _doctors_url = Column(String)
    address = Column(String)
    zip = Column(String)
    county = Column(String)
    city = Column(String)
    state = Column(Enum('AK', 'AL', 'AR', 'AS', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'GU', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'PR', 'PW', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY', name='state'))
    scrape_instruction = Column(JSONB(astext_type=Text()))


class Insurance(Base):
    __tablename__ = 'insurances'

    id = Column(Integer, primary_key=True, server_default=text("nextval('insurance_accepts_id_insurance_seq'::regclass)"))
    name = Column(Text, nullable=False, unique=True)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)


class KnexMigration(Base):
    __tablename__ = 'knex_migrations'

    id = Column(Integer, primary_key=True, server_default=text("nextval('knex_migrations_id_seq'::regclass)"))
    name = Column(String(255))
    batch = Column(Integer)
    migration_time = Column(DateTime(True))


class KnexMigrationsLock(Base):
    __tablename__ = 'knex_migrations_lock'

    index = Column(Integer, primary_key=True, server_default=text("nextval('knex_migrations_lock_index_seq'::regclass)"))
    is_locked = Column(Integer)


class Language(Base):
    __tablename__ = 'languages'

    id = Column(Integer, primary_key=True, server_default=text("nextval('language_id_language_seq'::regclass)"))
    name = Column(Text, nullable=False, unique=True)
    updated_at = Column(DateTime)
    created_at = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))


class PatientExperience(Base):
    __tablename__ = 'patient_experiences'

    id = Column(Integer, primary_key=True, server_default=text("nextval('rating_clinic_desc_id_rate_desc_seq'::regclass)"))
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(precision=3), nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))


class ProcedureCategory(Base):
    __tablename__ = 'procedure_categories'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)
    name = Column(String)
    parent_category = Column(ForeignKey('procedure_categories.id'))

    parent = relationship('ProcedureCategory', remote_side=[id])


class Specializatio(Base):
    __tablename__ = 'specializatios'

    id = Column(Integer, primary_key=True, server_default=text("nextval('sub_specialization_id_sub_specialization_seq'::regclass)"))
    name = Column(Text, nullable=False, unique=True)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)


class SpecialtyRanking(Base):
    __tablename__ = 'specialty_rankings'

    id = Column(Integer, primary_key=True, server_default=text("nextval('specialty_rankings_id_seq'::regclass)"))
    name = Column(Text, nullable=False, unique=True)
    description = Column(Text)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)


class Taxonomy(Base):
    __tablename__ = 'taxonomies'

    id = Column(Integer, primary_key=True, server_default=text("nextval('taxonomies_id_seq'::regclass)"))
    code = Column(String(13), nullable=False, unique=True)
    grouping = Column(String(98))
    classification = Column(String(122))
    specialization = Column(String(92))
    definition = Column(String(2615))
    effective_date = Column(Date)
    deactivation_date = Column(Date)
    last_modified_date = Column(Date)
    notes = Column(String(1829))
    display_name = Column(String(120))
    created_at = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)


class DoctorAward(Base):
    __tablename__ = 'doctor_awards'

    id = Column(Integer, primary_key=True, server_default=text("nextval('awards_honors_recognitions_id_seq'::regclass)"))
    doctor = Column(ForeignKey('doctors.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    name = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(precision=3), server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))

    doctor1 = relationship('Doctor')


class DoctorContact(Base):
    __tablename__ = 'doctor_contacts'

    id = Column(Integer, primary_key=True, server_default=text("nextval('address_id_seq'::regclass)"))
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)
    npi = Column(Integer)
    address = Column(String)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    address_type = Column(Enum('practice', 'mailing', 'secondary_practice', name='type_address'))
    telephone = Column(String)
    fax = Column(String)
    doctor = Column(ForeignKey('doctors.id'))

    doctor1 = relationship('Doctor')


class DoctorDuplicateNpi(Base):
    __tablename__ = 'doctor_duplicate_npi'

    id = Column(Integer, primary_key=True, server_default=text("nextval('doctors_duplicate_npi_id_seq'::regclass)"))
    npi = Column(Integer)
    doctor = Column(ForeignKey('doctors.id'), nullable=False)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)

    doctor1 = relationship('Doctor')


class DoctorEducation(Base):
    __tablename__ = 'doctor_educations'

    id = Column(Integer, primary_key=True, server_default=text("nextval('education_and_experience_id_education_seq'::regclass)"))
    name = Column(Text, nullable=False)
    doctor = Column(ForeignKey('doctors.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    created_at = Column(TIMESTAMP(precision=3), server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))
    description = Column(Text, nullable=False)

    doctor1 = relationship('Doctor')


class DoctorIdentifier(Base):
    __tablename__ = 'doctor_identifiers'

    id = Column(Integer, primary_key=True, server_default=text("nextval('doctors_identifiers_id_seq'::regclass)"))
    other_identifier = Column(String(255))
    type = Column(Enum('Medical', 'Other', name='type_code'))
    state = Column(String(255))
    issuer = Column(String(255))
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)
    doctor = Column(ForeignKey('doctors.id', ondelete='CASCADE', onupdate='CASCADE'))

    doctor1 = relationship('Doctor')


class DoctorInsurance(Base):
    __tablename__ = 'doctor_insurances'

    doctor = Column(ForeignKey('doctors.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    insurance = Column(ForeignKey('insurances.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    created_at = Column(TIMESTAMP(precision=3), server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))
    id = Column(Integer, primary_key=True, server_default=text("nextval('doctor_insurances_id_seq'::regclass)"))

    doctor1 = relationship('Doctor')
    insurance1 = relationship('Insurance')


class DoctorLanguage(Base):
    __tablename__ = 'doctor_languages'

    language = Column(ForeignKey('languages.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    doctor = Column(ForeignKey('doctors.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    created_at = Column(TIMESTAMP(precision=3), server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))
    id = Column(Integer, primary_key=True, server_default=text("nextval('doctors_languages_id_seq'::regclass)"))

    doctor1 = relationship('Doctor')
    language1 = relationship('Language')


class DoctorLicense(Base):
    __tablename__ = 'doctor_licenses'

    id = Column(Integer, primary_key=True, server_default=text("nextval('certification_and_license_id_certification_seq'::regclass)"))
    name = Column(Text, nullable=False)
    description = Column(Text, nullable=False)
    doctor = Column(ForeignKey('doctors.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    created_at = Column(TIMESTAMP(precision=3), server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))

    doctor1 = relationship('Doctor')


class DoctorPatientExperience(Base):
    __tablename__ = 'doctor_patient_experiences'

    doctor = Column(ForeignKey('doctors.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    patient_experience = Column(ForeignKey('patient_experiences.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    value = Column(Integer, nullable=False)
    created_at = Column(TIMESTAMP(precision=3), server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))
    id = Column(Integer, primary_key=True, server_default=text("nextval('doctors_overall_patient_experience_id_seq'::regclass)"))

    doctor1 = relationship('Doctor')
    patient_experience1 = relationship('PatientExperience')


class DoctorPublication(Base):
    __tablename__ = 'doctor_publications'

    id = Column(Integer, primary_key=True, server_default=text("nextval('publications_id_seq'::regclass)"))
    doctor = Column(ForeignKey('doctors.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    name = Column(Text, nullable=False)
    list_of_authors = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(precision=3), server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))

    doctor1 = relationship('Doctor')


class DoctorSpecialization(Base):
    __tablename__ = 'doctor_specializations'

    specialization = Column(ForeignKey('specializatios.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    doctor = Column(ForeignKey('doctors.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    created_at = Column(TIMESTAMP(precision=3), server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))
    id = Column(Integer, primary_key=True, server_default=text("nextval('doctors_subspecialization_id_seq'::regclass)"))

    doctor1 = relationship('Doctor')
    specializatio = relationship('Specializatio')


class DoctorTaxonomy(Base):
    __tablename__ = 'doctor_taxonomies'

    id = Column(Integer, primary_key=True, server_default=text("nextval('doctors_taxonomy_full_version_id_seq'::regclass)"))
    npi = Column(Integer)
    license = Column(String(255))
    state = Column(String(255))
    primary_switch = Column(Boolean)
    created_at = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)
    taxonomy = Column(ForeignKey('taxonomies.id', onupdate='CASCADE'), nullable=False)
    doctor = Column(ForeignKey('doctors.id', ondelete='CASCADE', onupdate='CASCADE'))

    doctor1 = relationship('Doctor')
    taxonomy1 = relationship('Taxonomy')


class HealthFacilityContact(Base):
    __tablename__ = 'health_facility_contacts'

    id = Column(Integer, primary_key=True, server_default=text("nextval('health_facility_contacts_id_seq'::regclass)"))
    npi = Column(Integer, nullable=False)
    address = Column(String)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    address_type = Column(Enum('practice', 'mailing', 'secondary_practice', name='type_address'))
    telephone = Column(String)
    fax = Column(String)
    created_at = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)
    health_facility = Column(ForeignKey('health_facilities.id', ondelete='CASCADE', onupdate='CASCADE'))

    health_facility1 = relationship('HealthFacility')


class HealthFacilityIdentifier(Base):
    __tablename__ = 'health_facility_identifiers'

    id = Column(Integer, primary_key=True, server_default=text("nextval('clinics_identifiers_id_seq'::regclass)"))
    other_identifier = Column(String(255))
    state = Column(String(255))
    issuer = Column(String(255))
    type = Column(Enum('Medical', 'Other', name='type_code'))
    created_at = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)
    health_facility = Column(ForeignKey('health_facilities.id', ondelete='CASCADE', onupdate='CASCADE'))

    health_facility1 = relationship('HealthFacility')


class HealthFacilityOtherName(Base):
    __tablename__ = 'health_facility_other_names'

    name = Column(String(150))
    type = Column(Enum('Doing Business As', 'Former Legal Business Name', 'Other Name', name='other_name_type'))
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)
    id = Column(Integer, primary_key=True, server_default=text("nextval('clinics_other_name_id_seq'::regclass)"))
    health_facility = Column(ForeignKey('health_facilities.id', ondelete='CASCADE', onupdate='CASCADE'))

    health_facility1 = relationship('HealthFacility')


class HealthFacilityTaxonomy(Base):
    __tablename__ = 'health_facility_taxonomies'

    id = Column(Integer, primary_key=True, server_default=text("nextval('clinics_taxonomy_id_seq'::regclass)"))
    license = Column(String(255))
    state = Column(String(255))
    primary_switch = Column(Boolean)
    created_at = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)
    taxonomy = Column(ForeignKey('taxonomies.id', onupdate='CASCADE'), nullable=False)
    health_facility = Column(ForeignKey('health_facilities.id', ondelete='CASCADE', onupdate='CASCADE'))

    health_facility1 = relationship('HealthFacility')
    taxonomy1 = relationship('Taxonomy')


class HospitalAssistanceEmployment(Base):
    __tablename__ = 'hospital_assistance_employments'

    hospital = Column(ForeignKey('hospitals.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    assistance_employment = Column(ForeignKey('assistance_employments.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    value = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(precision=3), nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))
    id = Column(Integer, primary_key=True, unique=True, server_default=text("nextval('clinics_main_service_id_seq'::regclass)"))

    assistance_employment1 = relationship('AssistanceEmployment')
    hospital1 = relationship('Hospital')


class HospitalDoctor(Base):
    __tablename__ = 'hospital_doctors'

    hospital = Column(ForeignKey('hospitals.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    doctor = Column(ForeignKey('doctors.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)
    id = Column(Integer, primary_key=True, unique=True, server_default=text("nextval('doctors_in_clinics_id_seq'::regclass)"))

    doctor1 = relationship('Doctor')
    hospital1 = relationship('Hospital')


class HospitalEmail(Base):
    __tablename__ = 'hospital_emails'

    id = Column(Integer, primary_key=True, server_default=text("nextval('clinic_emails_id_seq'::regclass)"))
    hospital = Column(ForeignKey('hospitals.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    email = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)

    hospital1 = relationship('Hospital')


class HospitalInHealthFacility(Base):
    __tablename__ = 'hospital_in_health_facilities'

    id = Column(Integer, primary_key=True, server_default=text("nextval('aha_hospital_in_health_facilities_id_seq'::regclass)"))
    hospital = Column(ForeignKey('hospitals.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    npi = Column(Integer)
    health_facility = Column(ForeignKey('health_facilities.id', ondelete='CASCADE', onupdate='CASCADE'))

    health_facility1 = relationship('HealthFacility')
    hospital1 = relationship('Hospital')


class HospitalPatientExperience(Base):
    __tablename__ = 'hospital_patient_experiences'
    __table_args__ = (
        Index('unq_patient_experience_clinic', 'hospital', 'patient_experience', unique=True),
    )

    hospital = Column(ForeignKey('hospitals.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    patient_experience = Column(ForeignKey('patient_experiences.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    created_at = Column(TIMESTAMP(precision=3), nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP(precision=3))
    value = Column(Integer, nullable=False)
    id = Column(Integer, primary_key=True, unique=True, server_default=text("nextval('clinics_patient_experience_id_seq'::regclass)"))

    hospital1 = relationship('Hospital')
    patient_experience1 = relationship('PatientExperience')


class HospitalPhone(Base):
    __tablename__ = 'hospital_phones'

    id = Column(Integer, primary_key=True, server_default=text("nextval('clinic_phones_id_seq'::regclass)"))
    hospital = Column(ForeignKey('hospitals.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    phone = Column(String(25), nullable=False)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)

    hospital1 = relationship('Hospital')


class HospitalProcedure(Base):
    __tablename__ = 'hospital_procedures'

    id = Column(Text, primary_key=True, server_default=text("random_prefix_id('hpc'::text, 24)"))
    hospital = Column(ForeignKey('hospitals.id'), nullable=False)
    cpt_hcpcs = Column(String(255))
    drg = Column(String(255))
    internal_code = Column(String(255))
    description = Column(Text)
    cost = Column(Float(53))
    max_ip = Column(Float(53))
    min_op = Column(Float(53))
    max_op = Column(Float(53))
    gross_charges = Column(Float(53))
    self_pay_cash_price = Column(Float(53))
    min_negotiated_rate = Column(Float(53))
    max_negotiated_rate = Column(Float(53))
    insurance_agent_price_in_patient = Column(Float(53))
    insurance_agent_price_out_patient = Column(Float(53))
    insurance_agent_medicaid_medicare = Column(Float(53))
    insurance_agent_price_in_some_plan = Column(Float(53))
    discounted_cash_price = Column(Float(53))
    payer_specific_negotiated_charge = Column(Float(53))
    de_identified_minimum_negotiated_charges = Column(Float(53))
    de_identified_maximum_negotiated_charges = Column(Float(53))
    date_update_clinic_price = Column(Float(53))
    other_data = Column(JSONB(astext_type=Text()))
    created_at = Column(DateTime(True), nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime(True), nullable=False, server_default=text("CURRENT_TIMESTAMP"))

    hospital1 = relationship('Hospital')


class HospitalSpecialtyRanking(Base):
    __tablename__ = 'hospital_specialty_rankings'

    id = Column(Integer, primary_key=True, server_default=text("nextval('clinic_specialty_rankings_id_seq'::regclass)"))
    hospital = Column(ForeignKey('hospitals.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    name = Column(Text, nullable=False)
    specialty = Column(ForeignKey('specialty_rankings.id'), nullable=False)
    value = Column(Text)
    updated_at = Column(TIMESTAMP(precision=0))
    created_at = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))

    hospital1 = relationship('Hospital')
    specialty_ranking = relationship('SpecialtyRanking')


class HospitalWebsite(Base):
    __tablename__ = 'hospital_websites'

    id = Column(Integer, primary_key=True, server_default=text("nextval('clinic_websites_id_seq'::regclass)"))
    hospital = Column(ForeignKey('hospitals.id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    website = Column(String, nullable=False)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)

    hospital1 = relationship('Hospital')


class ProcedureInfo(Base):
    __tablename__ = 'procedure_info'

    id = Column(Text, primary_key=True, server_default=text("random_prefix_id('hpi'::text, 16)"))
    hospital = Column(ForeignKey('hospitals.id'), nullable=False)
    procedure_describe = Column(Text)
    cost_date_update = Column(String(255))
    created_at = Column(DateTime(True), nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime(True), nullable=False, server_default=text("CURRENT_TIMESTAMP"))

    hospital1 = relationship('Hospital')


class Procedure(Base):
    __tablename__ = 'procedures'

    category = Column(ForeignKey('procedure_categories.id', onupdate='CASCADE'))
    name = Column(String, index=True)
    type = Column(Enum('drg', 'cpt/hcpcs', 'cpt', 'internal', 'hcpcs', name='type'))
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime)
    drg_type = Column(String)
    code = Column(String, index=True)

    procedure_category = relationship('ProcedureCategory')


class InsurancePrice(Base):
    __tablename__ = 'insurance_price'

    id = Column(Text, primary_key=True, server_default=text("random_prefix_id('ipc'::text, 24)"))
    hospital_procedure_id = Column(ForeignKey('hospital_procedures.id'), nullable=False)
    insurance_name = Column(String(255))
    insurance_packages = Column(String(255))
    insurance_type = Column(String(255))
    cost = Column(Float(53))
    created_at = Column(DateTime(True), nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(DateTime(True), nullable=False, server_default=text("CURRENT_TIMESTAMP"))

    hospital_procedure = relationship('HospitalProcedure')



# Connect to the PostgreSQL database
def main():
    engine = create_engine('postgresql+psycopg2://postgres:password@localhost/test_2')

    connection = engine.connect()
    Session = sessionmaker(bind = engine)
    session = Session()

    stmt = session.query(Hospital.id)

    table_name = Table('hospitals', metadata, autoload=True)
    new_column = Column('scrape_instruction', JSONB)
    alter_table = table_name.append_column(new_column)
    session.commit()

    data = pd.read_csv('parse_hospitals.csv', dtype={'id':'Int64'})
    data.dropna(subset='id', axis=0, inplace=True)
    data.set_index('id', inplace=True)

    for id, row in data.iterrows():
        scrape_instr = { 
            'url': row['link_to_file(array)'],
            'files': {
                'link_to_download': row['link_to_file_list'],
                'file_hash': ''
            }
        }
        result = json.dumps(scrape_instr)
        session.query(Hospital).\
           filter(Hospital.id == int(id)).\
           update({Hospital.scrape_instruction: result})
        session.commit()

    session.close()

if __name__ == "__main__":
    main()