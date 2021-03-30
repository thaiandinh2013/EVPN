import ast

import time
class DataQualityCheck:
    def __init__(self):
        self

    def quality_check(self, dict_line):
        dict_line = ast.literal_eval(dict_line)
        dict_line['error'] = []
        self.check_starttime(dict_line)
        self.check_start_state(dict_line)
        self.check_start_coordinate(dict_line)
        self.check_leg_1(dict_line)
        self.check_leg_2_to_20(dict_line)
        self.check_end_time(dict_line)
        self.check_end_state(dict_line)
        self.check_end_coordinate(dict_line)
        # print(dict_line)
        return dict_line

    def is_valid_json(self,dict_line):
        try:
            line = ast.literal_eval(dict_line)
            return True
        except:
            # print(dict_line)
            return False

    def check_starttime(self, dict_line):
        start_time = dict_line["start_time"]
        try:
            time.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            dict_line['error'].append('start_time')


    def check_start_state(self,dict_line):
        start_state = dict_line['start_state'].strip()
        if start_state not in ('LOADED' ,'UNLOADED'):
            dict_line['error'].append('start_state')

    def check_start_coordinate(self, dict_line):
        lat_lon =dict_line['start_coordinate'].strip('][').split(",")
        # print(lat_lon)
        if int(lat_lon[0]) > 180 or int(lat_lon[0])  < -180 or int(lat_lon[1]) >90 or int(lat_lon[0]) <-90:
            dict_line['error'].append('start_coordinate')


    def check_leg_1(self, dict_line):
        if 'leg_1' not in dict_line:
            dict_line['error'].append('leg_1')

    def check_end_time(self, dict_line):
        end_time = dict_line['end_time']
        try:
            time.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            dict_line['error'].append('end_time')

    def check_end_state(self,dict_line):
        start_state = dict_line['end_state'].strip()
        if start_state not in ['LOADED' ,'UNLOADED']:
            dict_line['error'].append('end_state')

    def check_end_coordinate(self, dict_line):
        lat_lon =dict_line['end_coordinate'].strip('][').split(",")
        if int(lat_lon[0]) > 180 or int(lat_lon[0])  < -180 or int(lat_lon[1]) > 90 or int(lat_lon[0]) < -90:
            dict_line['error'].append('end_coordinate')

    def check_leg_time(self,dict_line , leg, key ):
        if key not in dict_line[leg]:
            dict_line['error'].append(key)
        else:
            x_time = dict_line[leg][key]
            try:
                time.strptime(x_time, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                dict_line['error'].append(str(leg) + "_" +str(key))
    def check_leg_state(self,dict_line, leg,key):
        # print(dict_line)
        start_state = dict_line[leg][key].strip()
        # print(start_state)
        if start_state not in ['LOADED' ,'UNLOADED']:
            dict_line['error'].append(str(leg) + "_" +str(key))
    def check_leg_coordinate(self, dict_line,leg,key):
        # print(dict_line)
        # print(leg)
        # print(key)
        lat_lon =dict_line[leg][key].strip('][').split(',')
        # lat_lon =dict_line[leg][key]
        if int(lat_lon[0]) > 180 or int(lat_lon[0])  < -180 or int(lat_lon[1]) > 90 or int(lat_lon[0]) <- 90:
            dict_line['error'].append(str(leg) + "_" +str(key))

    def check_leg_2_to_20(self,dict_line):
        for i in range(2,21):
            leg = "leg_" + str(i)
            if leg not in dict_line:
                continue
            else:
                self.check_leg_time(dict_line,leg,"arrive_time")
                self.check_leg_state(dict_line , leg,"type")
                self.check_leg_time(dict_line , leg ,"leave_time")
                self.check_leg_coordinate(dict_line , leg , "coordinate")









