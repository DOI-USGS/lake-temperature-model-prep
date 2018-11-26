**source
copied from ftp://aftp.cmdl.noaa.gov/data/radiation/solrad/msn/README on 11/26/2018 for describing ftp://aftp.cmdl.noaa.gov/data/radiation/solrad/msn

DESCRIPTION OF ISIS DAILY DATA FILES 

FILENAME INFORMATION:

The files in these directories contain daily ASCII data from ISIS stations 
organized in Universal Coordinated Time (UTC).  The file naming convention is 
stayyjjj.dat, where sta is a three-letter station identifier, yy represents the 
last two digits of the year (i.e., 02 for 2002), and jjj is the day of the year (1-366).  
Days less than 100 are preceded by one or two zeros, e. g., day 75 would appear as 075, 
day 2 would appear as 002.  The ISIS processing software is year 2000 compliant.  
The year within the data files is written unambiguously with 4 digits on each line. 

"abq" is the station identifier for Albuquerque, New Mexico
"bis" is the station identifier for Bismarck, North Dakota
"hnx" is the station identifier for Hanford, California
"sea" is the station identifier for Seattle, Washington
"tlh" is the station identifier for Tallahassee, Florida
"slc" is the station identifier for Salt Lake City, Utah
"ste" is the station identifier for Sterling, Virginia
"msn" is the station identifier for Madison, Wisconsin
"ort" is the station identifier for Oak Ridge, Tennessee

For example, the file "abq02099.dat" contains data for Albuquerque
on day 99 of 2002.  

DATA STRUCTURE:

The data files are written in ASCII.  They may be read with the following 
FORTRAN code.  The format was specified in such a way to ensure that all 
entries are separated by at least one space so that the files may be read 
with free format.  Quality control parameters follow each measurement or 
calculated value.  ISIS follows the quality control (QC) philosophy of 
the BSRN.  Instead of deleting questionable data, they are flagged.  A QC flag 
of zero indicates that the corresponding data point is good, having passed all 
QC checks.  A value greater than 0 indicates that the data failed one level of 
QC.  For example, a QC value of 1 means that the recorded value is beyond 
the physically possible range for that measurement.  A value of 2 indicates 
that the data value failed the second level QC check, indicating that the 
data value may be physically possible, but should be used with scrutiny, and so  
on.  Missing values are indicated by -9999.9 and should always have a QC flag 
of 1. 

It is not unusual that thermopile-based solar pyranometers register a small 
negative signal at night. Most of this error is attributed to the thermopile
cooling to space.  

This generally affects the global and diffuse solar measurements, if thermopile-
based instruments are used for those measurements.  This error is also present
in daytime data, but its magnitude is difficult to determine.  Nighttime errors 
between 0 and -10 Wm^-2 are typical.  Because this behavior is common, only 
nighttime signals that drop below the common range are flagged.

Daytime diffuse measurements made by thermopile-based pyranometers are also 
adversely affected as much as the global measurements.  Because they are
shaded, the cosine error caused by the solar beam is precluded, and thus
Eppley model 8-48 pyranometer (notorious for its cosine error) can be used 
for the diffuse measurement. The model 8-48 pyranometer has effectively no offset.  
The model 8-48 pyranometer relies on a differential signal between a black and 
a white surface on the detector.  Since these two differently colored surfaces 
behave the same in the infrared, there is no error owing to radiative cooling of 
the sensor.  Therefore, the offset error will now only be seen in the global 
solar measurement. These instruments were installed during the instrument
exchanges of 2001.

On June 18, 2009, a pgrgeometer was added to the Madison complement of
instruments and the UVB instrument was removed.  After that date, Madison's
file format changed.

The following FORTRAN code may be used to read daily ISIS files EXCEPT
for Madison after 18 June 2009:

	parameter (nlines=480)
	character*80	station_name
c
	integer	year,month,day,jday,elevation,hr_to_LST,version
	integer	min(nlines),hour(nlines)
c
	real	latitude,longitude,dt(nlines),zen(nlines)
        real    direct(nlines)
	real	dw_psp(nlines)
	real	diffuse(nlines)
	real	uvb(nlines),uvb_temp_v(nlines)
c
	integer	qc_direct(nlines)
	integer	qc_dwpsp(nlines)
        integer qc_diffuse(nlines)
	integer	qc_uvb(nlines)
	integer	qc_uvb_temp(nlines)
c
c
        lun_in = 20
        open(unit=lun_in,file=[filename.dat],status='old',readonly)
c.
c.
c.
	read (lun_in,10) station_name
  10	format(1x,a)
	read(lun_in,*) latitude, longitude, elevation, hr_to_LST 
c
	icount = 0
	do i = 1,nlines
c	
	read(lun_in,30,end=40) year,jday,month,day,hour(i),min(i),dt(i),
     1	zen(i),dw_psp(i),qc_dwpsp(i),direct(i),qc_direct(i),
     2	diffuse(i),qc_diffuse(i),uvb(i),qc_uvb(i),uvb_temp(i),
     3  qc_uvb_temp(i),std_dw_psp(j),std_direct(j),std_diffuse(j),
     4  std_uvb(j)
c
	icount = icount + 1
c
  30  format(1x,i4,1x,i3,4(1x,i2),1x,f6.3,1x,f6.2,5(1x,f7.1,1x,i1),
     1 4(1x,f9.3))
c
	enddo
  40	type *,'end of file reached, ',icount,' records read'
.
.
.
c
c
c
c
c The following code can be used to read MADISON files AFTER 18 June 2009
c

	parameter (nlines=480)
	character*80	station_name
c
	integer	year,month,day,jday,elevation,hr_to_LST,version
	integer	min(nlines),hour(nlines)
c
	real	latitude,longitude,dt(nlines),zen(nlines)
        real    direct(nlines)
	real	dw_psp(nlines)
	real	diffuse(nlines)
	real	uvb(nlines)
	real    uvb_temp_v(nlines)
	real	dpir(nlines) !Downwelling IR in Wm^-2
	real	dpirc(nlines)!PIR case temperature (K)
	real	dpird(nlines)!PIR dome temperature (K)
c
	integer	qc_direct(nlines)
	integer	qc_dwpsp(nlines)
        integer qc_diffuse(nlines)
	integer	qc_uvb(nlines)
	integer	qc_uvb_temp(nlines)
	integer	qc_dpir(nlines)
	integer	qc_dpirc(nlines)
	integer	qc_dpird(nlines)
c
c
        lun_in = 20
        open(unit=lun_in,file=[filename.dat],status='old',readonly)
c.
c.
c.
	read (lun_in,10) station_name
  10	format(1x,a)
	read(lun_in,*) latitude, longitude, elevation, hr_to_LST 
c
	icount = 0
	do i = 1,nlines
c	
	read(lun_in,30,end=40) year,jday,month,day,hour(i),min(i),dt(i),
     1	zen(i),dw_psp(i),qc_dwpsp(i),direct(i),qc_direct(i),
     2	diffuse(i),qc_diffuse(i),uvb(i),qc_uvb(i),uvb_temp(i),
     3  qc_uvb_temp(i),dpir(i),qc_dpir(i),dpirc(i),qc_dpirc(i),dpird(i),
     4  qc_dpird(i),std_dw_psp(i),std_direct(i),std_diffuse(i),
     5  std_uvb(i),std_dpir(i),std_dpirc(i),std_dpird(i)
c
	icount = icount + 1
c
  30  format(1x,i4,1x,i3,4(1x,i2),1x,f6.3,1x,f6.2,8(1x,f7.1,1x,i1),
     1 7(1x,f9.3))
c
	enddo
  40	type *,'end of file reached, ',icount,' records read'
.
.
.

c
The file structure includes two header records; the first has the name of the 
station, and the second gives the station's latitude, longitude, elevation above 
mean sea level in meters, and the displacement in hours from local standard time.  
The files are organized in Universal Coordinated Time (UTC).  These are followed 
by at most, 480 lines of data, nominally from 0000 to 2357 UTC.  The date and time
parameters and solar zenith angle are given on every line.  Data are reported 
as 3-minute averages of one-second samples.  Reported times are the end 
times of the 3-min. averaging periods, i.e., the data given for 0000 UTC are
averaged over the period from 2357 (of the previous UTC day) to 0000.  
The solar zenith angle is reported in degrees on each line of data.  It is 
computed for the central time of the averaging period of the sampled data, i. e., 
1.5 minutes before the reported time on the line.  Missing-data periods 
within the files are not filled in with missing values, therefore, a file with
with missing periods will have fewer than 480 data records.  

Radiation values are reported to the tenths place.  Although this is beyond 
the accuracy of the instruments, data are reported in this manner in order to 
maintain the capability of backing out the raw voltages at the accuracy that 
they were originally reported.  Also, if the accuracies of the instruments 
improve to a fraction of a Wm^-2, the data format will not have to be changed.  

The variables, their data type, and description are given below:

station_name	character	station name, e. g., Goodwin Creek
latitude	real	latitude in decimal degrees (e. g., 40.80)
longitude	real	longitude in decimal degrees (e. g., 105.12)
elevation	integer	elevation above sea level in meters
h_to_lst	integer hours displaced from local standard time
year		integer	year, i.e., 2002
jday		integer	Julian day (1 through 365 [or 366])
month		integer	number of the month (1-12)
day		integer	day of the month(1-31)
hour		integer	hour of the day (0-23)
min		integer	minute of the hour (0-59)
dt		real	decimal time (hour.decimalminutes),e. g., 23.5 = 2330
zen		real	solar zenith angle (degrees)
dw_psp		real	downwelling global solar (Watts m^-2)
direct		real	direct solar (Watts m^-2)
diffuse		real	downwelling diffuse solar (Watts m^-2)
uvb		real	global UVB (milliWatts m^-2)
uvb_temp    	real	UVB temperature (C) -- 25 deg. C is normal
qc_dwpsp	integer quality control parameter for downwelling global solar (0=good)
qc_direct	integer quality control parameter for direct solar (0=good)
qc_diffuse	integer quality control parameter for diffuse solar (0=good)
qc_uvb		integer quality control parameter for UVB irradiance (0=good)
qc_uvb_temp	integer quality control parameter for the UVB instrument temperature (0=good)
std_dw_psp	real	standard deviation of the 1-sec. samples for global solar (dw_psp)
std_direct	real	standard deviation of the 1-sec. samples for direct solar
std_diffuse	real	standard deviation of the 1-sec. samples for diffuse solar
std_uvb		real	standard deviation of the 1-sec. samples for uvb

for MADISON AFTER 18 June 2009:

dpir		real    downwelling thermal IR (Watts m^-2)
dpirc		real    downwelling PIR case temperature (K)
dpird		real    downwelling PIR dome temperature (K)
std_dpir	real	standard deviation of the 1-sec. samples for downwelling IR
std_dpirc	real	standard deviation of the 1-sec. samples for PIR case temp
std_dpird	real	standard deviation of the 1-sec. samples for PIR dome temp



All radiation parameters, except UVB, are reported in units of Watts m^-2;
UVB is reported in milliWatts m^-2.

The UVB flux is given as the total measured surface UVB flux convoluted 
with the Diffey erythemal action spectrum, i.e., that part of the UVB spectrum 
responsible for sun burns on human skin (erythema) and DNA damage.  It is 
reported in this way because the response function of the UVB instrument 
approximates the erythemal action spectrum; thus the reported value is most 
representative of what the instrument is actually measuring.  

The temperature of the UVB instrument's filter is also reported in deg. C. 
25 deg. C is normal.  UVB irradiance values reported for instrument 
temperatures +/- 3 deg. C of the nominal value are not flagged.  If the 
instrument temperature is outside that range, the UVB measurement
is given a QC value of 2, and the QC value for the instrument temperature
is assigned a bad value (1).

The erythemal UVB irradiance reported in ISIS data files is computed for 
298 Dobson units of ozone.  This is done because the ozone value over the 
stations is unknown during the real-time processing.  If the ozone 
for a particular day is less than 300 Dobson units, then the reported UVB 
irradiance is less than it should be.   Tables to correct the reported 
values to the proper ozone may be obtained from SRRB.

The field UVB instruments are calibrated against a triad of "standard" 
instruments that are maintained by SRRB's UV calibration facility.  The 
standard instruments are periodically calibrated in the sun by comparing
their broadband measurements to the integrated output of UV spectroradiometers.  
These calibrations are transferred to the field instruments just before
they are deployed in the network by operating them side-by-side for a
few days.  A scale factor is computed for each day, which is simply a ratio of 
the test instrument's daily integral to that of the mean of the standards.  
The mean of the daily scale factors is used to transfer the standards' 
well maintained calibrations to the test instruments when they are deployed 
in the field.  

The standards' calibration factors are computed as a function of solar 
zenith angle, and are applied to the field instrument as such in the daily 
processing.  For example, to compute the UVB irradiance, the output voltage,
is multiplied times the Standards' calibration factor for the solar 
zenith angle that the measurement was made, then that result is divided by 
the scale factor for that field instrument.  

The following table lists the UVB Standards' calibration information computed
from  UV Spectroradiometer intercomparison data.  These, along with 
the scale factors computed for particular instruments are used to compute 
UVB erythemal(Diffey)-weighted irradiance for the ISIS field instruments.

erythemal    solar
conversion   zenith
factor       angle
(W m^-2 / V)

0.230        0.0 
0.231        5.0
0.232        10.0 
0.233        15.0
0.234        20.0
0.233        25.0
0.232        30.0
0.231        35.0
0.229        40.0
0.229        45.0
0.230        50.0
0.233        55.0
0.240        60.0
0.251        65.0
0.270        70.0
0.300        75.0
0.343        80.0
0.405        85.0
0.344        90.0


QUALITY CONTROL AND QUALITY ASSURANCE

These data are provisional.  SRRB has attempted to produce the best data set 
possible, however the data quality is constrained by measurement accuracies 
of the instruments and the quality of the calibrations.  Regardless, SRRB 
attempts to ensure the best quality possible through quality assurance and 
data quality control.  The data are subjected to automatic procedures as the
daily files are processed.  At present, they are subjected only to this
first-level check, and a daily "eye" check before being released,
however, as quality control procedures become more refined, they will 
be applied, and new versions of the data files will be generated.  

Quality assurance methods are in place to ensure against premature 
equipment failure in the field and post deployment data problems.  For 
example, all instruments at each station are exchanged for newly calibrated 
instruments on an annual basis.  Calibrations are performed by world-
recognized organizations.  For example, pyranometers and pyrheliometers are 
calibrated at the National Renewable Energy Laboratory (NREL).  
Calibration factors for the UVB instrument are transferred from three standards 
maintained by SRRB's National UV Calibration Facility in Boulder.  In general 
all of the standards the SRRB and NREL are traceable to NIST or its equivalent. 


THE INSTRUMENTS:

1.	The Solar Light UVB Broadband Radiometer

The conversion factor applied to the UVB data is valid for the solar spectral band 
from 280 to 320 nm and has a response function that approximates the erythemal (Diffey) 
action spectrum.  The conversion factor applied varies as a function of the solar 
zenith angle.  

2.	The Normal Incidence Pryheliometer (NIP)

The NIP measures direct solar radiation in the broadband spectral range from 
280 to 3000 nm.  Those used at ISIS stations are calibrated nominally 
on a biennial basis at the NOAA calibration facility in Boulder, CO

3.	Precision Spectral Pyranometer (PSP)

A PSP measures downwelling global irradiance at ISIS stations. 
These instruments are sensitive to the same broadband 
spectral range as the NIP, 280 to 3000 nm.  They are calibrated 
on a biennial basis at the NOAA calibration facility in Boulder, CO

An inherent problem with the PSP is that its sensor cools to space (if it
is directed upward) and this causes a negative signal.  This is apparent at 
night where it shows up as an erroneous negative irradiance  that can be as
high as -30 Wm^-2.  Daytime data also contain this error but it is masked by 
the large solar signal.  This error especially affects the diffuse measurement 
made by a PSP because the sensor is shaded.  Therefore, the PSP is not used
for the diffuse measurement.  

4.	Eppley 8-48 "black and white" pyranometer 

This pyranometer is used for the diffuse solar measurement.  It has been found 
to have desirable properties for this measurement because it does not use a 
thermopile for the detector and thus does not have the "nighttime" offset problem.  
These instruments are sensitive to the same broadband spectral range as the NIP 
and PSP, 280 to 3000 nm.  They are also calibrated on a biennial basis at 
the NOAA calibration facility in Boulder, CO

5.      Precision Infrared Radiometer (PIR) 

The PIR measures and downwelling thermal infrared irradiance at Madison only.
It is sensitive to the spectral range from 3000 to 50,000 nm.  NOAA maintains
three standard PIRs that are calibrated annually by a world-reputable
organization.  These standards are used to calibrate field PIRs.
