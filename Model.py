from config import *
import pymysql
import datetime

class Model():
    Name = ""
    Text = ""
    Schema = ""
    #init
    def __init__(self, name):
        self.Name  = name
        self.Dbcon = pymysql.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PASS,MYSQL_DBNAME)
        self.fields = []
        self.values = []
        self.types  = []

    # set Text value
    def set(self, values):
        self.values = values
        self.reformat()
        self.getValues()

    # Run mysql query
    def runQuery(self, query):
        dbcur = self.Dbcon.cursor()
        try:
            dbcur.execute(query)
            self.Dbcon.commit()
        except:
            self.Dbcon.rollback()
            print(query)
            print("SQL statement Error")
        dbcur.close()

    # reformat the fields' name or values
    def reformat(self):
        for i in range(len(self.values)):
            self.values[i] = self.Dbcon.escape_string(self.values[i].replace('"','').replace('\n','').replace("'","''"))

    # get Date from text
    def getDate(self, text):
        try:
            return datetime.datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
        except:
            print(text)
            return '0000-00-00'

    # get values from text
    def getValues(self):
        for i in range(0, len(self.types)):
            if 'INT' in self.types[i] or 'DOUBLE' in self.types[i]:
                if self.values[i]=='':self.values[i]='0'
                self.values[i]= float(self.values[i].strip())
            elif 'DATE' in self.types[i]:
                self.values[i] = self.getDate(self.values[i])



    # create a table from header of csv
    def creatTable(self):
        # check if table is existed or not
        if not self.checkTableExists(self.Name):
            query = """CREATE TABLE `%s` ( `%s` int(10) NOT NULL,""" %(self.Name, self.fields[0])
            for i in range(1, len(self.fields)):
                query += "`%s` %s,"% (self.fields[i], self.types[i])
            query += """PRIMARY KEY (`%s`) ) ENGINE=InnoDB DEFAULT CHARSET=latin1;""" % (self.fields[0])
            # create table with name
            self.runQuery(query)


    # func to check table existance
    def checkTableExists(self, tablename):
        dbcur = self.Dbcon.cursor()
        dbcur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{0}'
            """.format(tablename.replace('\'', '\'\'')))
        if dbcur.fetchone()[0] == 1:
            dbcur.close()
            return True
        dbcur.close()
        return False

    # save model to db
    def save(self):

        # check duplicate id
        dbcur = self.Dbcon.cursor()
        dbcur.execute(
            """SELECT COUNT(*) FROM `%s` WHERE `TM_NUMBER` = %s LIMIT 1""" %(self.Name, self.values[0])
        )
        if not dbcur.fetchone()[0]:
            query = """INSERT into `%s` ( %s ) VALUES (""" % (self.Name, ",".join(self.fields))
            for i in range(len(self.fields)):
                if 'INT' in self.types[i]:
                    query += """%s,"""%(self.values[i])
                else:
                    query += """'%s',"""%(self.values[i])
            query = query[0:len(query)-1] + ')'
        else:
            query = """UPDATE `%s` SET """ %(self.Name)
            for i in range(1, len(self.fields)):
                if 'INT' in self.types[i]:
                    query += """`%s`= %s,""" % (self.fields[i], self.values[i])
                else:
                    query += """`%s`= '%s',""" % (self.fields[i],self.values[i])
            query = query[0:len(query)-1] + """ where `TM_NUMBER`=%s""" % (self.values[0])
        dbcur.close()
        # run query
        self.runQuery(query)


class IPGOLD201(Model):

    def __init__(self):
        super(IPGOLD201, self).__init__('IPGOLD201')
        self.types=[
            "INT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "DATE",
            "DATE",
            "TEXT",
            "INT(2) DEFAULT NULL",
            "INT(5) DEFAULT NULL",
            "INT(10) DEFAULT NULL",
            "INT(10) DEFAULT NULL",
            "INT(10) DEFAULT NULL"
        ]
        self.fields = [
            "tm_number",
            'type_of_mark_code',
            'cpi_status_code',
            'live_or_dead_code',
            'trademark_type',
            'madrid_application_indicator',
            'lodgement_date',
            'registered_from_date',
            'country',
            'Australian',
            'entity',
            'applicant_no',
            'lodgement_year',
            'registered_from_year'
        ]


class IPGOLD202(Model):

    def __init__(self):
        super(IPGOLD202, self).__init__('IPGOLD202')
        self.types =[
            "INT",
            "INT(10) DEFAULT NULL",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "INT(2) DEFAULT NULL",
            "INT(2) DEFAULT NULL",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "INT(10) DEFAULT NULL",
            "DOUBLE  DEFAULT NULL",
            "DOUBLE  DEFAULT NULL",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "TEXT",
            "TEXT",
            "INT(10) DEFAULT NULL",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "TEXT",
            "INT(5) DEFAULT NULL"
        ]

        self.fields = [
            "tm_number",
            "ipa_id",
            "fmr_ipa_id",
            "country",
            "australian",
            "entity",
            "name",
            "cleanname",
            "corp_desg",
            "state",
            "postcode",
            "lat",
            "lon",
            "sa2_code",
            "sa2_name",
            "lga_code",
            "lga_name",
            "gcc_name",
            "elect_div",
            "abn",
            "acn",
            "entity_type",
            "applicant_type",
            "big"
        ]

class IPGOLD203(Model):

    def __init__(self):
        super(IPGOLD203, self).__init__('IPGOLD203')
        self.types = [
            "INT",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "DATE",
            "DATE",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "DATE",
            "DATE",
            "DATE",
            "DATE",
            "DATE",
            "INT(10) DEFAULT NULL",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "DATE",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "INT(10) DEFAULT NULL",
            "INT(10) DEFAULT NULL",
            "INT(10) DEFAULT NULL",
            "DATE",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT"
        ]
        self.fields = [
            "tm_number",
            "status_code",
            "status_code_desc",
            "type_of_mark_code",
            "expedite_flag_ind",
            "non_use_flag_ind",
            "cpi_status_code",
            "live_or_dead_code",
            "trademark_type",
            "lodgement_date",
            "PRIORITY_DATE__DIVISIONAL_DATE",
            'kind_colour_ind',
            'kind_scent_ind',
            'kind_shape_ind',
            'kind_sound_ind',
            'acceptance_due_date',
            'sealing_due_date',
            'registered_from_date',
            'sealing_date',
            'renewal_due_date',
            'divisional_number',
            'part_assign_parent_number',
            'priority_number',
            'priority_country_code',
            'section45_application_code',
            'court_orders_ind',
            'ir_number_notify_date',
            'ir_number',
            'ir_number_extension_value',
            'part_transform_parent_number',
            'transform_ir_number',
            'transform_ir_extension_no',
            'ir_renewal_due_date',
            'act1955_reg_acpt_code',
            'revocation_acpt_pend_ind',
            'gs_assistance_ind',
            'lodgement_type_code',
            'madrid_application_indicator'
        ]

class IPGOLD204(Model):

    def __init__(self):
        super(IPGOLD204, self).__init__('IPGOLD204')
        self.types = [
            "INT",
            "INT(10) DEFAULT NULL",
            "INT(10) DEFAULT NULL",
            "TEXT"
        ]
        self.fields = [
            'tm_number',
            'class_code',
            'occ_num',
            'description_text'
        ]

class IPGOLD206(Model):

    def __init__(self):
        super(IPGOLD206, self).__init__('IPGOLD206')
        self.types = [
            "INT",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT"
        ]

        self.fields = [
            'tm_number',
            'self_filed',
            'name',
            'cleanname',
            'country',
            'state',
            'firm_id'
        ]

class IPGOLD207(Model):

    def __init__(self):
        super(IPGOLD207, self).__init__('IPGOLD207')
        self.types = [
            "INT",
            "INT(10) DEFAULT NULL",
            "INT(10) DEFAULT NULL",
        ]

        self.fields = [
            'tm_number',
            'report_no',
            'class_count'
        ]


class IPGOLD208(Model):

    def __init__(self):
        super(IPGOLD208, self).__init__('IPGOLD208')
        self.types = [
            "INT",
            "INT(10) DEFAULT NULL",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "INT(10) DEFAULT NULL",
            "DATE",
            "DATE",
            "DATE",
            "DATE",
            "DATE",
            "DATE",
            "TEXT",
            "DATE",
            "DATE",
            "DATE",
            "TEXT",
            "DATE",
            "DATE",
            "DATE",
            "TEXT",
            "DATE",
            "DATE",
            "TEXT",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "TEXT",
            "DATE",
            "DATE",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "TEXT",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "TEXT",
            "DATE",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "DATE",
            "DATE",
            "INT(10) DEFAULT NULL",
            "DATE",
            "TEXT",
            "DATE",
            "DATE",
            "INT(10) DEFAULT NULL",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "TEXT",
            "DATE",
            "DATE",
            "DATE",
            "INT(10) DEFAULT NULL",
            "DATE",
            "INT(10) DEFAULT NULL",
            "DATE",
            "DATE"
        ]

        self.fields = [
            'tm_number',
            'opp_seq_no',
            'opposition_status_code',
            'opposition_type',
            'opposition_code',
            'non_use_lodged_date',
            'opp_extension_date',
            'opp_lodged_date',
            'ev_support_extension_date',
            'ev_support_lodged_date',
            'ev_support_served_date',
            'ev_support_type',
            'ev_answer_extension_date',
            'ev_answer_lodged_date',
            'ev_answer_served_date',
            'ev_answer_type',
            'ev_reply_extension_date',
            'ev_reply_lodged_date',
            'ev_reply_served_date',
            'ev_reply_type',
            'withdrawal_lodged_date',
            'referred_date',
            'person_referred_code',
            'opp_act_year',
            'status_text',
            'prop_agent_msg_text',
            'opp_created_date',
            'opp_modified_date',
            'opp_status_line_1_text',
            'opp_status_line_2_text',
            'opp_status_line_3_text',
            'opp_status_line_4_text',
            'opp_status_line_5_text',
            'opp_status_line_6_text',
            'opp_status_line_7_text',
            'hearing_number',
            'hearing_type',
            'hearing_code',
            'decided_date',
            'decision_type',
            'pending_type',
            'resume_date',
            'appeal_lodged_date',
            'hearing_status_code',
            'deferred_date',
            'hear_stat_desc_text',
            'hear_created_date',
            'hear_modified_date',
            'opp_evi_seq_no',
            'opp_evidence_status',
            'opp_evidence_type',
            'opp_evidence_code',
            'amend_enter_date',
            'corro_lodgment_date',
            'ev_served_date',
            'amendment_no',
            'final_appeal_date',
            'opp_evi_act_year',
            'opp_evi_created_date',
            'opp_evi_modified_date'
        ]

class IPGOLD220(Model):

    def __init__(self):
        super(IPGOLD220, self).__init__('IPGOLD220')
        self.types = [
            "INT",
            "INT(10) DEFAULT NULL",
            "DATE",
            "TEXT",
            "TEXT",
            "DATE"
        ]

        self.fields = [
            'tm_number',
            'ci_number',
            'approval_date',
            'ci_text',
            'header_tm_case_no',
            'last_amend_date'
        ]


class IPGOLD221(Model):
    def __init__(self):
        super(IPGOLD221, self).__init__('IPGOLD221')
        self.types = [
            "INT",
            "INT(10) DEFAULT NULL",
            "TEXT",
            "TEXT"
        ]

        self.fields = [
            'tm_number',
            'occ_number',
            'endorsement_text',
            'endorsement_type'
        ]


class IPGOLD222(Model):
    def __init__(self):
        super(IPGOLD222, self).__init__('IPGOLD222')
        self.types = [
            "INT",
            "TEXT",
            "TEXT"
        ]

        self.fields = [
            'tm_number',
            'trademark_text',
            'device_phrase_text'
        ]
