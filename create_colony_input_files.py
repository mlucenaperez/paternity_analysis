# SCRIPT FOR GENERATING COLONY INPUT FILES FROM A DEMOGRAPHIC DATABASE IN SQL #

import os, string, re, psycopg2
import unicodedata # to handle unicodes
import subprocess 
import itertools
from config import *
		

def ConnectToPostgres(host='localhost'):	
	try:
		conn = psycopg2.connect('dbname=%s user=postgres host=%s password= %s' % (database,host, password))
		cursor = conn.cursor()
		return conn,cursor
	except:
		return None, None

def ExecPGQuery(cursor,ex_val,paramlist,parm=None):
	try:
		if parm is None:
			cursor.execute(ex_val,paramlist)
		else:
			cursor.execute(ex_val,paramlist,parm)
		return True
	except:
		return False


def Die(msg): # error sms if it cannot contact
	print msg + '\nPress ^Z to quit'
	connection.close()
	raise SystemExit

def update_genotypes ():
	"""Update the individuals that are genotyped based on micros database"""

	sql = """with selection as (select distinct id_lab from lp_micros_id_lab)
	update lp_gralinfo set micros_genotyped = '1' from selection where selection.id_lab=lp_gralinfo.id_lab; """

	if not ExecPGQuery(cursor,sql,[]):
		Die('\n\ncould not execute Postgres query1\n\n')				
	
def get_genotypes (year): 
	"""Given a year, it selects genotipes of the offspring, father and mother following our criteria"""
	
	sql = '''SELECT lp_micros_name.* FROM lp_gralinfo, lp_micros_name 
	WHERE lp_gralinfo.name = lp_micros_name.name and 
	birth_pop = 'don' and micros_genotyped = true and assigned_birth_date = %i;'''  % (year)
	
	if not ExecPGQuery(cursor,sql,[]):
		Die('\n\ncould not execute Postgres query2\n\n')				
	offspring = cursor.fetchall ()
		

	offspring_str = ('\n'.join(''.join(str(item)) for item in offspring))
	offspring_str = offspring_str.replace("'", "").replace (",","").replace ("(", "").replace(")", "")	
	

	sql = '''SELECT lp_micros_name.* FROM lp_gralinfo, lp_micros_name 
	WHERE lp_gralinfo.name = lp_micros_name.name and 
	sex = 'm' and current_pop = 'don' and micros_genotyped = TRUE
	and assigned_birth_date <= %i  and assigned_birth_date >= %i and (death_date >= %i or death_date is null);'''  % (year-2, year-12, year)
		
	if not ExecPGQuery(cursor,sql,[]):
		Die('\n\ncould not execute Postgres query3\n\n')				
	father = cursor.fetchall ()
	
		
	father_str = ('\n'.join(''.join(str(item)) for item in father))
	father_str = father_str.replace("'", "").replace (",","").replace ("(", "").replace(")", "")	


	sql = '''SELECT lp_micros_name.* FROM lp_gralinfo, lp_micros_name 
	WHERE lp_gralinfo.name = lp_micros_name.name and 
	sex = 'h' and current_pop = 'don' and micros_genotyped = TRUE
	and assigned_birth_date <= %i  and assigned_birth_date >= %i and (death_date >= %i or death_date is null);'''  % (year-2, year-12, year)
	
	if not ExecPGQuery(cursor,sql,[]):
		Die('\n\ncould not execute Postgres query4\n\n')				
	mother = cursor.fetchall ()
	
	
	mother_str = ('\n'.join(''.join(str(item)) for item in mother))
	mother_str = mother_str.replace("'", "").replace (",","").replace ("(", "").replace(")", "")	
	
		
	return offspring, father, mother, offspring_str, father_str, mother_str


def get_number_of_alleles_per_locus():

	sql ='''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'lp_micros_name';'''
	if not ExecPGQuery(cursor,sql,[]):
		Die('could not execute Postgres query5')
	micros_list = cursor.fetchall()
	
	micros_list = list(itertools.chain(*micros_list)) # convert tupla or list of list in simple list
	del micros_list[0] # rm the first element of the list

	return micros_list 

	


def generate_colony_parameters (year, scenario, offspring, father, mother, micros_list): 
	
	input1 = str('don_relax_'+ str(year) + '_scn_' + str(scenario)) +'\n' # Proyect name
	input1 += str ('don_relax_'+ str(year) + '_scn_' + str(scenario)) +'\n'# Output name
	input1 += str (len(offspring)) + '\n'  ## Number of offspring in the sample
	input1 += str (len(micros_list)/2) + '\n'  ## Number of loci 
	input1 += '1234\n' # Seed for random number generator
	input1 += '0\n' # Not updating allele freq
	input1 += '2\n' # Dioecious species
	input1 += '0\n' # Diploid species
	input1 += '0\n' # imbreeding
	
	if scenario == 1 or scenario == 3 or scenario == 5:
		input1 += str (0)+ '\t'+ str(0) + '\n' # Polygamy (0)/ monogamy (1) for males and females in this case: males poligamous and females poligamous
	elif scenario == 2 or scenario == 4 or scenario == 6:
		input1 += str (0)+ '\t'+ str(1) + '\n' # Polygamy (0), monogamy (1) for males and females in this case: males poligamous and females monogamous
	else:
		Die('\n\nNo scenario given 1\n\n')
		
	input1 += str ('0') + '\n' # No clone inference
	input1 += str ('0') + '\n' # Scale full sibship NO
	input1 += str ('0') + '\t'+str (1.0) + '\t'+ str (1.0)+'\n' # 0, 1, 2, 3= no, weak, medium strong sibship size prior; mean paternal and maternal sibship size
	input1 += str ('1') + '\n' # known population allele freq
	
	##########################################################################
	
	input2 = str ('1') + '\n' # Number of run
	input2 += str ('2') + '\n' # Length of run: Medium
	input2 += str ('0') + '\n' # Monitor method by iterate
	input2 += str ('100000') + '\n' # Monitor interval in iterate
	input2 += str ('0') + '\n' # Non-window version
	input2 += str ('1') + '\n' # Fulllikelihood
	input2 += str ('2') + '\n' # Medium precision fo full-likelihood
	
	##########################################################################
	
	input3 = str ('0.9') + '\t' + str ('0.9') + '\n'### prob that the father and mother of an offspring are included in candidates
	input3 += str (len(father)) + '\t' + str (len(mother)) ### Numbers of candidate males and females
		
	return input1, input2, input3

def load_allele_frequencies(main_dir, year):
	
	folder = "%s/allele_freq" % (main_dir) 
	
        if year <= 2006:
		allele_freq = open ('%s/allfreq_1990_2006.txt' % (folder), 'r')
		allele_freq = allele_freq.read()
	elif year == 2007:
		allele_freq = open ('%s/allfreq_2007.txt' % (folder), 'r')
		allele_freq = allele_freq.read()
	elif year > 2007 and year < 2010:
			allele_freq = open ('%s/allfreq_2008_2009.txt' % (folder), 'r')
			allele_freq = allele_freq.read()	
	elif year >= 2010:
		allele_freq = open ('%s/allfreq_2010_2012.txt' % (folder), 'r')
		allele_freq = allele_freq.read()
	else:
		Die('\n\nNo allele freq given for year %i\n\n' % (year))
		
				
	lista = allele_freq.split("\n")		
		
	counter = 0
	number_alleles= []
	
	for item in lista:
			counter += 1
			if counter%2==0:
					number_alleles.append(item.strip().count("\t")+1)
	
	number_alleles = ' '.join(str(e) for e in number_alleles)
	number_alleles = number_alleles.replace("'", "")
		
		
	allele_freq = allele_freq.replace(","," ")
	allele_freq = allele_freq.replace(";"," ")
	allele_freq = allele_freq.replace("\t"," ")
	
	return allele_freq, number_alleles
	
	
def load_allele_dropout_rate (main_dir):
	
	folder = "%s/allele_dropout" % (main_dir)
	allele_dropout_rate = open ('%s/allele_dropout.txt' % (folder), 'r') 
	allele_dropout_rate = allele_dropout_rate.read()
	allele_dropout_rate = allele_dropout_rate.replace(","," ")
	allele_dropout_rate = allele_dropout_rate.replace(";"," ")
	allele_dropout_rate = allele_dropout_rate.replace("\t"," ")
	return allele_dropout_rate	
	
	
def load_sibship (main_dir, scenario, year):
	
	if scenario == 1 or scenario == 2 or scenario == 5 or scenario == 6:
		return None
	elif scenario == 3 or scenario == 4:
		folder = "%s/known_sibship" % (main_dir)
		if os.path.exists('%s/known_sibship_%i.txt' % (folder, year)):
			sibship = open ( "%s/known_sibship_%i.txt" % (folder, year), 'r')
			sibship = sibship.read()
			sibship = sibship.replace(","," ")
			sibship = sibship.replace(";"," ")
			sibship = sibship.replace("\t"," ")
			
			return sibship
		else:
			return "STOP"
	else:
		Die('\n\nNo scenario given 2\n\n')	
	
def load_sibship_extra (main_dir, scenario, year):
	
	if scenario == 1 or scenario == 2 or scenario == 3 or scenario == 4:
		return None
	elif scenario == 5 or scenario == 6:
		folder = "%s/known_sibship" % (main_dir)
		if os.path.exists('%s/known_sibship_%i_extra.txt' % (folder, year)):
			sibship = open ( "%s/known_sibship_%i_extra.txt" % (folder, year), 'r')
			sibship = sibship.read()
			sibship = sibship.replace(","," ")
			sibship = sibship.replace(";"," ")
			sibship = sibship.replace("\t"," ")
			
			return sibship
		else:
			return None
	else:
		Die('\n\nNo scenario given 2\n\n')	
		
		

def load_mother_sibship (main_dir, scenario, year):
	
	if scenario == 1 or scenario == 2 or scenario == 3 or scenario == 4:
		return None
	elif scenario == 5 or scenario == 6:
		folder = "%s/known_mother_sibship" % (main_dir)
		if os.path.exists('%s/known_mother_sibship_%i.txt' % (folder, year)):
			mother_sibship = open ( "%s/known_mother_sibship_%i.txt" % (folder, year), 'r')
			mother_sibship = mother_sibship.read().replace(","," ").replace(";"," ").replace("\t"," ")
			
			return mother_sibship
		else:
			return "STOP"	
	else:
		Die('\n\nNo scenario given 3\n\n')	
		
		

def generate_colony_input (main_dir, input1, number_alleles, allele_freq, input2, allele_dropout_rate, offspring_str, input3, \
		                                         mother_str, father_str, sibship, sibship_extra, mother_sibship, scenario, year):
			
	folder = "%s/colony_input" %(main_dir)
	folder2 = "%s/known_sibship" % (main_dir)
	
			
	if scenario == 1 or scenario == 2:
		
		colony_input_total = open('%s/don_relax_%i_%i.txt' % (folder, scenario, year), 'w')
		colony_input_total.write(str(input1) + '\n' + str(number_alleles) + '\n' + str(allele_freq) + '\n' + str(input2) + '\n' + str(allele_dropout_rate) + '\n' + str(offspring_str) + '\n' 
		                         + str(input3) + '\n' + str(father_str) + '\n' + str(mother_str) + '\n' + '0' + '\n' + '0' + '\n' + '0' + '\n'
		                         + '0' + '\n' + '0'+ '\n' + '0'+ '\n' + '0'+ '\n' + '0')
		colony_input_total.close ()		
		
	elif scenario == 3 or scenario == 4:
		
		if sibship == "STOP":
				generate_colony_EMPTY_input (main_dir, scenario, year)
				print "Sibship not available"		
		else:
		
			colony_input_total = open('%s/don_relax_%i_%i.txt' % (folder, scenario, year), 'w')
			colony_input_total.write(str(input1) + '\n' + str(number_alleles) + '\n' + str(allele_freq) + '\n' + str(input2) + '\n' + str(allele_dropout_rate) + '\n' + str(offspring_str) + '\n' 
				                 + str(input3) + '\n' + str(father_str) + '\n' + str(mother_str) + '\n' + '0' + '\n' + '0' + '\n' + '0' + '\n'
				                 + str(sibship) + '\n' + '0'+ '\n' + '0'+ '\n' + '0'+ '\n' + '0')
			colony_input_total.close ()		
		
	elif scenario == 5 or scenario == 6:
		
		if mother_sibship == "STOP":
				generate_colony_EMPTY_input (main_dir, scenario, year)
				print "Mother sibship not available"	
				
		elif os.path.exists('%s/known_sibship_%i_extra.txt' % (folder2, year)):
			
			colony_input_total = open('%s/don_relax_%i_%i.txt' % (folder, scenario, year), 'w')
			colony_input_total.write(str(input1) + '\n' + str(number_alleles) + '\n' + str(allele_freq) + '\n' + str(input2) + '\n' + str(allele_dropout_rate) + '\n' + str(offspring_str) + '\n' 
		                                 + str(input3) + '\n' + str(father_str) + '\n' + str(mother_str) + '\n' + '0' + '\n' + str(mother_sibship) + '\n' + '0' + '\n' + str(sibship_extra) + '\n'
		                                 + '0'+ '\n' + '0'+ '\n' + '0'+ '\n' + '0')
			colony_input_total.close ()				
			
					
			
		
		else:
			colony_input_total = open('%s/don_relax_%i_%i.txt' % (folder, scenario, year), 'w')
			colony_input_total.write(str(input1) + '\n' + str(number_alleles) + '\n' + str(allele_freq) + '\n' + str(input2) + '\n' + str(allele_dropout_rate) + '\n' + str(offspring_str) + '\n' 
		                                 + str(input3) + '\n' + str(father_str) + '\n' + str(mother_str) + '\n' + '0' + '\n'  + str(mother_sibship) + '\n' + '0' + '\n' + '0' + '\n'
		                                  + '0'+ '\n' + '0'+ '\n' + '0'+ '\n' + '0')
			colony_input_total.close ()			
		
	else:
		print "Problem generating input"
	
	
	
def generate_colony_EMPTY_input (main_dir, scenario, year):
			
	folder = "%s/colony_input" %(main_dir)
	colony_input_total = open('%s/empty_don_relax_%i_%i.txt' % (folder, scenario, year), 'w')
	colony_input_total.write("Sibship not available for doing this scenario")
	colony_input_total.close ()	



#BEGIN################################################


# Connect to Postgres
connection,cursor = ConnectToPostgres()
if connection is None:
	Die('Could not connect to Postgres, use SSH and run program again.')


# Generate input db and running colony


update_genotypes()


sql = '''SELECT distinct assigned_birth_date from lp_gralinfo where birth_pop = 'don' 
and current_pop = 'don' and micros_genotyped = TRUE and assigned_birth_date >= 1990 order by assigned_birth_date;'''
	# CUIDADO!!! si el id ya no está en don no nos va a salir!!
if not ExecPGQuery(cursor,sql,[]):
	Die('could not execute Postgres query7')
years = cursor.fetchall()
years = list(itertools.chain(*years))  # Te convierte una tupla o una lista de listas en una lista simple. 


scenarios = {}	
scenarios[1] = {}
scenarios[1]['monogamy'] = 0  
scenarios[1]['known_sibship'] = 0
scenarios[1]['known_mother'] = 0 
scenarios[2] = {}
scenarios[2]['monogamy'] = 1  
scenarios[2]['known_sibship'] = 0
scenarios[2]['known_mother'] = 0 
scenarios[3] = {}
scenarios[3]['monogamy'] = 0  
scenarios[3]['known_sibship'] = 1
scenarios[3]['known_mother'] = 0 
scenarios[4] = {}
scenarios[4]['monogamy'] = 1  
scenarios[4]['known_sibship'] = 1
scenarios[4]['known_mother'] = 0 
scenarios[5] = {}
scenarios[5]['monogamy'] = 0  
scenarios[5]['known_sibship'] = 1
scenarios[5]['known_mother'] = 1
scenarios[6] = {}
scenarios[6]['monogamy'] = 1  
scenarios[6]['known_sibship'] = 1
scenarios[6]['known_mother'] = 1


micros_list = get_number_of_alleles_per_locus()	


for year in years:
	for scenario in scenarios.keys():
		print "Year: "+str(year)+ " Scenario: " + str(scenario) 
		offspring, father, mother, offspring_str, father_str, mother_str = get_genotypes(year)
		input1, input2, input3 = generate_colony_parameters (year, scenario, offspring, father, mother, micros_list)
		allele_freq, number_alleles = load_allele_frequencies (main_dir, year)
		allele_dropout_rate = load_allele_dropout_rate (main_dir)
		sibship = load_sibship (main_dir, scenario, year)
		sibship_extra = load_sibship_extra (main_dir, scenario, year)
		mother_sibship  = load_mother_sibship (main_dir, scenario, year)
		
		
		Input_File_Name = generate_colony_input (main_dir, input1, number_alleles, allele_freq, input2, allele_dropout_rate, offspring_str, input3, \
		                                         mother_str, father_str, sibship, sibship_extra, mother_sibship, scenario, year)


connection.commit()			
connection.close()


print 'DONE!'

#END#
