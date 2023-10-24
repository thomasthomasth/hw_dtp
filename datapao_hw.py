# Databricks notebook source
# Load standard python packages

import csv
from datetime import datetime
import typing as T
import os
import sys


class homework():
  def __init__(self, task_name):
    self.activities : T.Dict[str, T.List[T.Dict[str, T.Any]]] = None
    self.df : T.List[T.Dict[str, T.Any]] = None
    self.task_name : str = task_name
    self.header : T.List[str] = ['user_id', 'time', 'days', 'average_per_day', 'rank'] if task_name == 'task1' else ['user_id', 'session_length']
  
  def read_csv(self):
    # Collect the content of the csv file into a list of dictionaries. (key, value) ==def (user_id, logged events)

    activities = {}

    # Open the CSV file
    with open('./input/datapao_homework_2023.csv', 'r') as file:
        # Create a CSV reader object
        csv_reader = csv.reader(file)

        # Skip the header row
        next(csv_reader)

        # Iterate through the logs
        for row in csv_reader:
          user_id = row[0]
          event_type = row[1]
          event_time = row[2]
          event_time_ts = datetime.strptime(event_time,"%Y-%m-%dT%H:%M:%S.%fZ")
          if user_id not in activities.keys():
            activities.update({user_id : [{'event_type' : event_type, 'event_time' : event_time_ts}]})
          else:
            activities[user_id].append({'event_type' : event_type, 'event_time' : event_time_ts})
      
    self.activities = activities

  def task(self):
    
    if self.task_name == 'task1':
      # Task1: For each person, calculate the amount of time and the number of days spent in the office during February and write it to a CSV file containing (user_id, time, days, average_per_day, rank)

      df = []

      for user_id, timestamps in self.activities.items():
        
        # Net Hours

        net_seconds = 0
        days = []
        for i in range(1,len(timestamps)):
          if timestamps[i]['event_type'] == 'GATE_OUT' and timestamps[i]['event_time'].year == 2023 and timestamps[i]['event_time'].month == 2:
            time_delta = timestamps[i]['event_time'] - timestamps[i-1]['event_time']
            net_seconds += time_delta.total_seconds()
            day = timestamps[i]['event_time'].date()
            days.append(day)
  
        average_per_day = (net_seconds/60/60)/len(set(days))

        row = {'user_id' : user_id, 'time' : net_seconds, 'days' : len(set(days)), 'average_per_day' : average_per_day}

        df.append(row)

      df_with_avg_per_day = sorted(df, key=lambda x: x['average_per_day'])

      self.df = [{**value, 'rank' : i+1} for i, value in enumerate(df_with_avg_per_day)]

    else:
      df_task2 = []

      for user_id, acts in self.activities.items():
          session_length = 0
          max_session_length = 0
    
          for i, act in enumerate(acts[1:]):
            event_timestamp = act['event_time']
            event_type = act['event_type']
            if event_timestamp.year == 2023 and event_timestamp.month == 2:
              if event_type.lower() == 'gate_in':
                time_delta = acts[1:][i]['event_time'] - acts[i-1]['event_time']
                time_delta_seconds = time_delta.total_seconds()
                if time_delta_seconds < 60*60*2:
                 session_length += time_delta.total_seconds()
                elif max_session_length < session_length:
                  max_session_length = session_length
                else:
                 session_length = 0
          df_task2.append({'user_id' : user_id, 'session_length' : max_session_length})

      self.df = [sorted(df_task2, key=lambda x: x['session_length'])[-1]]

  def write_to_csv(self):
    
    os.makedirs("output", exist_ok=True) 
    with open(f'output/{self.task_name}.csv', 'w', newline='') as file:

      writer = csv.DictWriter(file, fieldnames=self.header)

      # Write the header
      writer.writeheader()

      # Write the data
      for row in self.df:
          writer.writerow(row)

    print(f'Data written to {self.task_name}.csv')

def test_source_file_exists():
  assert os.path.exists('./input/datapao_homework_2023.csv') == True, "The source file doesn't exist"

def test_event_types():
  howework_instance = homework('task1')
  howework_instance.read_csv()
  assert set([event['event_type'].lower() for key, value in howework_instance.activities.items() for event in value]) == set(['gate_in', 'gate_out']), "The event type should be either gate_in or gate_out (in lowercase)"







