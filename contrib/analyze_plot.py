#!/usr/bin/env python3
# (C) 2017, Markus Wildi, wildi.markus@bluewin.ch
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 2, or (at your option)
#   any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software Foundation,
#   Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#   Or visit http://www.gnu.org/licenses/gpl.html.
#

'''

Analyze and plot oxymetry and pulse rate data

'''

__author__ = 'wildi.markus@bluewin.ch'

import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
import collections
import os
import glob
from pathlib import Path
import subprocess
import argparse
import logging
import itertools
toggle=itertools.cycle([0,-1])

font = {'family': 'serif',
        'color':  'darkred',
        'weight': 'normal',
        'size': 12,
}

class Script(object):

    def __init__(
            self,
            lg=None,
            break_after=None,
            cmd_cms50dp=None,
            device_cms50dp=None,
            path_cms50dp=None,
            name_cms50dp=None,
            path_audio=None,
            name_audio=None,
            upper_spo2=None,
            lower_spo2=None,
            minimum_spo2=None,
            duration_spo2=None,
            offline_length_ringbuffer=None,
            difference_minimum_pulse_rate=None,
            maximum_time_difference=None,
            ):

        self.lg=lg
        self.break_after=break_after
        self.cmd=cmd_cms50dp
        self.device_cms50dp=device_cms50dp
        self.path_cms50dp=path_cms50dp
        self.name_cms50dp=name_cms50dp
        self.path_audio=path_audio
        self.name_audio=name_audio
        self.upper_spo2=upper_spo2
        self.lower_spo2=lower_spo2
        self.minimum_spo2=minimum_spo2
        self.duration_spo2=duration_spo2
        self.offline_length_ringbuffer=offline_length_ringbuffer
        self.difference_minimum_pulse_rate=difference_minimum_pulse_rate
        self.maximum_time_difference=maximum_time_difference
    
        self.dt_spo2_low_bgs=list()
        self.dt_spo2_low_ends=list()
        self.spo2_bgs=list()
        self.spo2_ends=list()
        self.last_pr_mns=list()
        self.dt_last_pr_mns=list()
        self.pulse_rate_mns=list()
        self.pulse_rate_mxs=list()
        self.dt_pulse_rate_mns=list()
        self.dt_pulse_rate_mxs=list()
        self.playtimes=list()

        self.dt_begin=None
        self.df=None
        self.dt_str_start=None
        self.pthfn_data=None
        self.pthfn_spo2=None
        self.pthfn_pulse=None
        self.pthfn_playtimes=None
        
    def expand_fn(self):
        if self.name_cms50dp is None:
            snd_fn=Path(max(glob.iglob('{}/*.mp3'.format(self.path_cms50dp)), key=os.path.getctime))
            fn,fe=os.path.splitext(snd_fn.name)
            self.dt_str_start=fn
        else:
            fn,fe=os.path.splitext(self.name_cms50dp)
        
        self.pthfn_spo2=os.path.join(self.path_cms50dp,'{}_spo2.csv'.format(fn))
        self.pthfn_pulse=os.path.join(self.path_cms50dp,'{}_pulse.csv'.format(fn))
        self.pthfn_playtimes=os.path.join(self.path_cms50dp,'{}_playtimes.csv'.format(fn))
        self.pthfn_data=os.path.join(self.path_cms50dp,'{}.csv'.format(fn))
        self.lg.info('expand_fn: data file {}'.format(self.pthfn_data))
            
    def retrieve_cms50dp_data(self):
        self.expand_fn()
        # ./cms50dplus.py RECORDED /dev/ttyUSB0 2017-05-24T00:00:0.csv -s "2017-05-24T00:00:00"
        cmd=list()
        cmd.append(self.cmd)
        cmd.append('RECORDED')
        cmd.append(self.device_cms50dp)
        cmd.append(self.pthfn_data)
        cmd.append('-s')
        cmd.append(self.dt_str_start)

        self.lg.debug('retrieve_cms50dp_data: {}'.format(' '.join( x for x in cmd)))
        
        stdo,stde=subprocess.Popen(cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        if stdo:
            self.lg.debug('stdo: {}'.format(stdo))
        if stde:
            self.lg.error('stde: {}'.format(stde))
                                                

    def analyze_cms50dp_data(self):

        self.expand_fn()
        dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        self.df=pd.read_csv(self.pthfn_data,skiprows=[0], header=None,names=['date_time','pulse_rate','spo2',], parse_dates=['date_time'],date_parser=dateparse,index_col='date_time')
        # spo2 analysis
        dt_spo2_bg=None
        dt_pr_bg=None
        spo2_mn=100.
        spo2_pr_mn=500.
        spo2_pr_mx=0.
        #
        last_pr_mn=500.
        dt_last_pr_mn=None

        # pulse analysis
        pulse_rate_mx=0.
        pulse_rate_mn=300.
        dt_pulse_rate_mx=None
        dt_pulse_rate_mn=None
        
        ring_buffer_spo2 = collections.deque(maxlen=self.offline_length_ringbuffer)
        ring_buffer_pulse = collections.deque(maxlen=self.offline_length_ringbuffer)

        for i,dt in enumerate(self.df.index.to_pydatetime()):
            if self.dt_begin is None:
                self.dt_begin=dt
            if i > self.break_after:
                break
                
            ring_buffer_spo2.append(dt)
            ring_buffer_pulse.append(dt)
            dfr=self.df.ix[str(dt)]
            pulse_rate=dfr.iloc[0]['pulse_rate']
            spo2=dfr.iloc[0]['spo2']

            if spo2 < spo2_mn:
                spo2_mn=spo2
        
            if spo2 < self.lower_spo2 and dt_spo2_bg is None:
                spo2_bg=spo2
                dt_spo2_bg=dt
            elif spo2 > self.upper_spo2 and dt_spo2_bg is not None:
                if pulse_rate < spo2_pr_mn:
                    spo2_pr_mn=pulse_rate
                elif pulse_rate > spo2_pr_mx:
                    spo2_pr_mx=pulse_rate
            
                d_dt=dt-dt_spo2_bg
                # filter
                if d_dt.total_seconds() > self.duration_spo2 and spo2_mn < self.minimum_spo2:
                    # find last minimum
                    last_pr_mn=500.
                    dt_last_pr_mn=None
                    for dt_rbf in  reversed(ring_buffer_spo2):
                        pr_act=self.df.ix[str(dt_rbf)].iloc[0]['pulse_rate']
                        if dt_rbf > dt_spo2_bg:
                            continue
                        if pr_act < last_pr_mn:
                            last_pr_mn=pr_act
                            dt_last_pr_mn= dt_rbf
                        elif dt_last_pr_mn is not None and pr_act > last_pr_mn +5: #ToDo make an option
                            self.last_pr_mns.append(last_pr_mn)
                            self.dt_last_pr_mns.append(dt_last_pr_mn)
                            break
                    else:
                        self.last_pr_mns.append(None)
                        self.dt_last_pr_mns.append(None)
                        self.lg.debug('no minimum found: {}, {}'.format(dt_rbf, pr_act))
                        #ax.vlines(x=dt_rbf, ymin=pr_act-15, ymax=pr_act+5, color='c')
                        
                
                    seconds_begin= dt_spo2_bg-self.dt_begin
                    if dt_last_pr_mn is not None:
                        d_dt_last_pr_mn= dt_spo2_bg-dt_last_pr_mn
                        self.lg.debug('spo2: {},{}, spo2_mn: {}, dur: {}, pr_mn/mx: {}/{}, last pr_mn: {}, at sec before: {}, d_pr: {}'.format(dt_spo2_bg,seconds_begin.total_seconds(),spo2_mn,d_dt.total_seconds(),spo2_pr_mn,spo2_pr_mx,last_pr_mn, d_dt_last_pr_mn.total_seconds(), spo2_pr_mx-last_pr_mn))
                    else:
                        self.lg.debug('spo2: {},{},{},{},{}'.format(dt_spo2_bg,seconds_begin.total_seconds(),spo2_mn,d_dt.total_seconds(),spo2_pr_mn,spo2_pr_mx))
                    
                    seconds_end= dt- self.dt_begin
                    self.playtimes.append([seconds_begin.total_seconds()-60.,seconds_end.total_seconds()+60.])
                    self.dt_spo2_low_bgs.append(dt_spo2_bg)
                    self.dt_spo2_low_ends.append(dt)
                    self.spo2_bgs.append(spo2_bg)
                    self.spo2_ends.append(spo2)
                    last_pr_mn=500.
                    dt_last_pr_mn=None
                    ring_buffer_spo2 = collections.deque(maxlen=self.offline_length_ringbuffer)
            
                spo2_mn=100.
                dt_spo2_bg=None
                spo2_pr_mn=500.
                spo2_pr_mx=0.

            elif dt_spo2_bg is not None:
                if pulse_rate < spo2_pr_mn:
                    spo2_pr_mn=pulse_rate
                elif pulse_rate > spo2_pr_mx:
                    spo2_pr_mx=pulse_rate

            # pulse rate search min and following max 
            if pulse_rate <= pulse_rate_mn:
                dt_pulse_rate_mn=dt
                pulse_rate_mn=pulse_rate
                pulse_rate_mx=0.
                dt_pulse_rate_mx=None
            elif dt_pulse_rate_mn is not None and pulse_rate >= pulse_rate_mx:
                dt_pulse_rate_mx=dt
                pulse_rate_mx=pulse_rate
            elif dt_pulse_rate_mn is not None and dt_pulse_rate_mx is not None:
                pr_d=pulse_rate_mx-pulse_rate_mn
                d_dt_pr=dt_pulse_rate_mx-dt_pulse_rate_mn
                if (dt-dt_pulse_rate_mn).total_seconds() > self.maximum_time_difference:
                    pulse_rate_mx=0.
                    pulse_rate_mn=300.
                    dt_pulse_rate_mx=None
                    dt_pulse_rate_mn=None
                else:
                    if d_dt_pr.total_seconds() < self.maximum_time_difference:
                        if pr_d > self.difference_minimum_pulse_rate:
                            self.lg.debug('pulse: {}, {}, {} '.format(dt_pulse_rate_mn,pr_d,d_dt_pr.total_seconds()))
                            self.pulse_rate_mns.append(pulse_rate_mn)
                            self.pulse_rate_mxs.append(pulse_rate_mx)
                            self.dt_pulse_rate_mns.append(dt_pulse_rate_mn)
                            self.dt_pulse_rate_mxs.append(dt_pulse_rate_mx)
                
                            pulse_rate_mx=0.
                            pulse_rate_mn=300.
                            dt_pulse_rate_mx=None
                            dt_pulse_rate_mn=None

                        
    def display_spo2(self,ax=None):
        for i,dt_spo2_low_bg in enumerate(self.dt_spo2_low_bgs):
            ymin=max(self.spo2_ends[i],70)
            ymax=min(self.spo2_ends[i]+20,102)
            self.dt_spo2_low_ends
            ax.vlines(x=dt_spo2_low_bg, ymin=self.spo2_bgs[i], ymax=ymax, color='r')
            ax.vlines(x=self.dt_spo2_low_ends[i], ymin=ymin, ymax=ymax, color='g')
            ax.hlines(y=ymax, xmin=dt_spo2_low_bg, xmax=self.dt_spo2_low_ends[i], color='b')
            if self.dt_last_pr_mns[i] is not None:
                if self.dt_last_pr_mns[i] not in self.dt_spo2_low_bgs:
                    ax.vlines(x=self.dt_last_pr_mns[i], ymin=self.last_pr_mns[i], ymax=ymax, color='m')
                    ax.hlines(y=ymax, xmin=self.dt_last_pr_mns[i], xmax=dt_spo2_low_bg, color='b')

            ax.text(self.dt_spo2_low_bgs[i],ymax+next(toggle), '{} sec'.format((self.dt_spo2_low_bgs[i]-self.dt_begin).total_seconds()),fontdict=font)

    def display_pulse(self,ax=None):
        for i,pulse_rate_mn in enumerate(self.pulse_rate_mns):
            ymin_mn=pulse_rate_mn-15
            ymax_mn=pulse_rate_mn+5
            pr_d=self.pulse_rate_mxs[i]-pulse_rate_mn
            d_dt_pr=self.dt_pulse_rate_mxs[i]-self.dt_pulse_rate_mns[i]
            ax.vlines(x=self.dt_pulse_rate_mns[i], ymin=ymin_mn, ymax=ymax_mn, color='g')
            ax.vlines(x=self.dt_pulse_rate_mxs[i], ymin=ymin_mn, ymax=self.pulse_rate_mxs[i], color='r')
            ax.text(self.dt_pulse_rate_mxs[i],ymin_mn, '{} sec, {} db, {} sec'.format((self.dt_pulse_rate_mxs[i]-self.dt_begin).total_seconds(),pr_d,d_dt_pr.total_seconds()),fontdict=font)
            ax.hlines(y=ymin_mn, xmin=self.dt_pulse_rate_mns[i], xmax=self.dt_pulse_rate_mxs[i], color='b')
    
    def plot_analysis(self):
        ax=self.df.plot()
        self.display_spo2(ax=ax)
        self.display_pulse(ax=ax)
        plt.show()
        
    def store_spo2(self):
        df=pd.DataFrame({'dt_spo2_low_bg':self.dt_spo2_low_bgs,'self.dt_spo2_low_ends':self.dt_spo2_low_ends,'spo2_bg':self.spo2_bgs,'spo2_end':self.spo2_ends,'dt_last_pr_mn':self.dt_last_pr_mns,'last_pr_mn':self.last_pr_mns})
        sdf= df[['dt_spo2_low_bg','self.dt_spo2_low_ends','spo2_bg','spo2_end','dt_last_pr_mn','last_pr_mn']]
        sdf.to_csv(self.pthfn_spo2)               
    
    def store_pulse(self):
        df=pd.DataFrame({'dt_pulse_rate_mn':self.dt_pulse_rate_mns,'pulse_rate_mn':self.pulse_rate_mns,'dt_pulse_rate_mx':self.dt_pulse_rate_mxs, 'pulse_rate_mx': self.pulse_rate_mxs})
        sdf= df[['dt_pulse_rate_mn','pulse_rate_mn', 'dt_pulse_rate_mx', 'pulse_rate_mx']]
        sdf.to_csv(self.pthfn_pulse)               


    
    def store_playtimes(self):
        start=[x[0] for x in self.playtimes]
        end=[x[1] for x in self.playtimes]
        duration=[(x[1]-x[0]) for x in self.playtimes]
        df=pd.DataFrame({'start': start,'end': end,'duration': duration })
        sdf= df[['start','end','duration']]
        sdf.to_csv(self.pthfn_playtimes)               
    
    def log_playtimes(self):
        for ele in self.playtimes:
            self.lg.info('file {}, start: {}, end: {}, duration: {}'.format(self.pthfn_data,ele[0],ele[1], (ele[1]-ele[0])))



if __name__ == "__main__":

    parser= argparse.ArgumentParser(prog=sys.argv[0], description='Analyze SpO2 and Pulse of an CMS50D+ device, read out by https://github.com/atbrask/CMS50Dplus')

    parser.add_argument('--level', dest='level', default='WARN', help=': %(default)s, debug level')
    parser.add_argument('--toconsole', dest='toconsole', action='store_true', default=False, help=': %(default)s, log to console')
    parser.add_argument('--break_after', dest='break_after', action='store', default=100000000, type=int, help=': %(default)s, read max. positions, mostly used for debuging')
    parser.add_argument('--log-path', dest='log_path', action='store', default='/tmp/',type=str, help=': %(default)s, directory where log files is stored')

    subparsers = parser.add_subparsers(help='commands', dest='command')
    retrieve_parser = subparsers.add_parser('retrieve', help='retrieve data from CMS50D+ device analyze')
    retrieve_parser.add_argument('--cmd-cms50dp', dest='cmd_cms50dp', action='store', default='./CMS50Dplus/cms50dplus/cms50dplus.py',type=str, help=': %(default)s, full path to CMS50D+ Python scripts')
    retrieve_parser.add_argument('--device-cms50dp', dest='device_cms50dp', action='store', default='/dev/ttyUSB0',type=str, help=': %(default)s, full path to CMS50D+ USB device')
    retrieve_parser.add_argument('--path-cms50dp', dest='path_cms50dp', action='store', default='./data',type=str, help=': %(default)s, path where to store CMS50D+ file')
    retrieve_parser.add_argument('--name-cms50dp', dest='name_cms50dp', action='store', default=None,type=str, help=': %(default)s, CMS50D+ data file name, None: %%Y-%%m-%%dT%%H:%%M:%%S.csv')
    retrieve_parser.add_argument('--path-audio', dest='path_audio', action='store', default='./data',type=str, help=': %(default)s, path to audio file(s)')
    retrieve_parser.add_argument('--name-audio', dest='name_audio', action='store', default=None,type=str, help=': %(default)s, audio file name')
    
    file_parser = subparsers.add_parser('file', help='analyze CMS50D+ data from file')
    file_parser.add_argument('--path-cms50dp', dest='path_cms50dp', action='store', default='./data',type=str, help=': %(default)s, path to CMS50D+ file(s)')
    file_parser.add_argument('--name-cms50dp', dest='name_cms50dp', action='store', default=None,type=str, help=': %(default)s, CMS50D+ data file name')
    # 
    file_parser.add_argument('--path-audio', dest='path_audio', action='store', default='./data',type=str, help=': %(default)s, path to audio file(s)')
    file_parser.add_argument('--name-audio', dest='name_audio', action='store', default=None,type=str, help=': %(default)s, audio file name')

    parser.add_argument('--upper-spo2', dest='upper_spo2', action='store', default=89.,type=float, help=': %(default)s %%, level begin apnaoe')
    parser.add_argument('--lower-spo2', dest='lower_spo2', action='store', default=87.,type=float, help=': %(default)s %%, level end apnoe')
    parser.add_argument('--minimum-spo2', dest='minimum_spo2', action='store', default=87.,type=float, help=': %(default)s \%%, upper minimum level')
    parser.add_argument('--duration-spo2', dest='duration_spo2', action='store', default=15.,type=float, help=': %(default)s sec, minimum duration to be apnoe')
    parser.add_argument('--offline-length-ringbuffer', dest='offline_length_ringbuffer', action='store', default=120,type=int, help=': %(default)s sec, maximum time period to search for an pulse rate minimum')

    parser.add_argument('--difference-minimum-pulse-rate', dest='difference_minimum_pulse_rate', action='store', default=12.,type=float, help=': %(default)s beat, heart beat diffference minimum')
    parser.add_argument('--maximum-time-difference', dest='maximum_time_difference', action='store', default=45.,type=float, help=': %(default)s sec, maximum time difference where the raise of the heart beat occurs')

    args=parser.parse_args()

    if args.toconsole:
        args.level='DEBUG'
    
    if not os.path.exists(args.log_path):
        os.makedirs(args.log_path)
    
    pth, fn = os.path.split(sys.argv[0])
    filename=os.path.join(args.log_path,'{}.log'.format(fn.replace('.py',''))) # ToDo datetime, name of the script
    logformat= '%(asctime)s:%(name)s:%(levelname)s:%(message)s'
    logging.basicConfig(filename=filename, level=args.level.upper(), format= logformat)
    logger = logging.getLogger()
    
    if args.toconsole:
        # http://www.mglerner.com/blog/?p=8
        soh = logging.StreamHandler(sys.stdout)
        soh.setLevel(args.level)
        soh.setLevel(args.level)
        logger.addHandler(soh)

    dev=None
    cmd=None
    if args.command == 'retrieve':
        cmd=args.cmd_cms50dp
        dev=args.device_cms50dp
        
    sc=Script(lg=logger,
              break_after=args.break_after,
              cmd_cms50dp=cmd,
              device_cms50dp=dev,
              path_cms50dp=args.path_cms50dp,
              name_cms50dp=args.name_cms50dp,
              path_audio=args.path_audio,
              name_audio=args.name_audio,
              upper_spo2=args.upper_spo2,
              lower_spo2=args.lower_spo2,
              minimum_spo2=args.minimum_spo2,
              duration_spo2=args.duration_spo2,
              offline_length_ringbuffer=args.offline_length_ringbuffer,
              difference_minimum_pulse_rate=args.difference_minimum_pulse_rate,
              maximum_time_difference=args.maximum_time_difference)
    
    if args.command == 'retrieve':
        sc.retrieve_cms50dp_data()
        
    sc.analyze_cms50dp_data()
    sc.plot_analysis()
    #sc.log_playtimes()
    sc.store_spo2()
    sc.store_pulse()
    sc.store_playtimes()
