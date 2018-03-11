#!/usr/bin/env python3

###############################################################################
# PBS PRO Accounting Log - Job history
###############################################################################
# History
# 08.03.2018  FilipVV   Initial version created
###############################################################################

import sys, time, argparse

VERSION = "1.0"

log_details={}
totals={}
hist_1h={}

jobmask = {
            'B' : 'Reservation Record',
            'C' : 'Job checkpointed & requeued',
            'D' : 'Job/Subjob deleted',
            'E' : 'Job/Subjob ended',
            'K' : 'Reservation terminated by owner',
            'L' : 'Information about floating licenses',
            'M' : 'Job move record',
            'P' : 'Provisioning starts for job/reservation',
            'p' : 'Provisioning ends for job/reservation',
            'Q' : 'Job entered queue',
            'R' : 'Job rerun',
            'S' : 'Job execution started',
            'T' : 'Job restarted from checkpoint file',
            'U' : 'Unconfirmed reservation',
            'Y' : 'Confirmed reservation by scheduler'
         }

             

def init_stats_1h():
   """Initialize hourly statistics""" 
   for h in range(0,24):
       for jobstat in jobmask:
           hist_1h[(h,jobstat)]=0

   for jobstat in jobmask:
       totals[jobstat]=0




def print_legend():
    """Print PBS accounting log legend"""
    #--------------------------------------------------------------------------
    # Print Legend
    #---------------------------------------------------------------------------
    print("\n\nLegend:")
    for jobstat in sorted(jobmask):
        print('{:>3} = {:>}'.format(jobstat,jobmask[jobstat]))
    print()




def print_ratios():
    """Print all ratios"""
    #--------------------------------------------------------------------------
    # Print ratios
    #---------------------------------------------------------------------------
    print("\nRatios:")
    print("  Single Machine Jobs ended.... {:>} of {:>} [E] is {:>.2f}%".format(
             totals['single_node_jobs'],
             totals['E'],
             totals['single_node_jobs'] / totals['E'] * 100))
    



def print_statistics():
    """"Print the requested statistics"""

    line='----|------------------------------------------------' \
      +  '---------------------------------------------------------'

    period=time.strftime('%a %d-%m-%Y',log_details['date'])

    #--------------------------------------------------------------------------
    # Print header line
    #--------------------------------------------------------------------------
    print("\nPBS Accounting log...: {:>}".format(log_details['file']))
    print("Date/Time period.....: {:>}\n".format(period))
    print('    | ',end='')
    for jobstat in sorted(jobmask):
        print('{:>6} '.format(jobstat), end='')

    print()

    #--------------------------------------------------------------------------
    # Print statistics
    #--------------------------------------------------------------------------
    print(line)
    for h in range (0, 24):
        print(' {:0>2} | '.format(h),end='');

        for jobstat in sorted(jobmask):
            print('{:>6} '.format(hist_1h[(h,jobstat)]),end='')

        print()

    print(line)


    #--------------------------------------------------------------------------
    # Print summary line
    #--------------------------------------------------------------------------
    print('    | ',end='')
    for jobstat in sorted(jobmask):
        print('{:>6} '.format(totals[jobstat]),end='')

    print()
    print(line)

    


def get_account_log(pbs_accountlog,queue):
    """Read accounting logfile and generate requested histograms"""
    #--------------------------------------------------------------------------
    # Initialisation
    #--------------------------------------------------------------------------
    log_details['file']=pbs_accountlog
    cnt_single_node_job=0

    init_stats_1h()

    try:
       F = open(pbs_accountlog,'r')
    except OSError:
       print('Could not open PBS accounting log "{:>}"'.format(pbs_accountlog))
       sys.exit()

   
    #--------------------------------------------------------------------------
    # Get Date of accounting file
    #-------------------------------------------------------------------------- 
    first=F.readline().split(';') 
    d = time.strptime(first[0],'%m/%d/%Y %H:%M:%S')
    log_details['date']=d   

    #--------------------------------------------------------------------------
    # Start processing all lines in file
    #--------------------------------------------------------------------------
    F.seek(0)
    for line in F:
        col=line.rstrip().split(';')

        tstamp     = col[0] 
        jobstat    = col[1] 
        jobid      = col[2]
        accounting = col[3]

        #----------------------------------------------------------------------
        # Build dictionary of key/value pairs on accounting data
        #----------------------------------------------------------------------
        accounting=accounting.replace('domain users','domain_users')
        #print(line)
        try:
            acc_dict = dict(item.split('=',1) for item in accounting.split())
        except ValueError:
            pass


        #----------------------------------------------------------------------
        # Generate statistics
        #----------------------------------------------------------------------
        d = time.strptime(tstamp,'%m/%d/%Y %H:%M:%S')
        key=(d.tm_hour,jobstat)
        if key in hist_1h:
           hist_1h[key] += 1
        else:
           hist_1h[key] = 1

        #----------------------------------------------------------------------
        # Ad Hoc
        #----------------------------------------------------------------------
        if jobstat == 'E' and acc_dict['Resource_List.nodect'] == '1':
           cnt_single_node_job+=1

    #--------------------------------------------------------------------------
    # Generate totals
    #--------------------------------------------------------------------------
    for jobstat in sorted(jobmask):
        for h in range (0, 24):
            totals[jobstat]+=hist_1h[(h,jobstat)]
   
    totals['single_node_jobs'] = cnt_single_node_job

      



###############################################################################

#------------------------------------------------------------------------------
# Parse Command line arguments
#------------------------------------------------------------------------------
parser = argparse.ArgumentParser(
             description='PBS PRO Accounting log checker',
             epilog='v1.0 by Filip van Vooren - G-FSI/ELC')

parser.add_argument("accounting_log",    help="PBS Accounting log to process")
parser.add_argument("-v", "--version",   help="Show script version")
parser.add_argument("-q", "--queue",     help="Filter on queue", default="*")                 
parser.add_argument("-nr", "--noratios", help="Don't show ratios", action="store_true")
parser.add_argument("-nl", "--nolegend", help="Don't show legend", action="store_true")

args=parser.parse_args()



#------------------------------------------------------------------------------
# Generate statistics
#------------------------------------------------------------------------------
get_account_log(args.accounting_log,queue=args.queue)
print_statistics()

if not args.noratios:
   print_ratios()

if not args.nolegend:
   print_legend()
