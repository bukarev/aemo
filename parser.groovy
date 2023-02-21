import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.Duration;

byte[] bytes = new byte[10240];
String[] rCodes = {};

// Sample Groovy code
records = sdc.records
for (record in records) {

  recList = new ArrayList();

  try {
      
      f_Filename = record.attributes['filename'];
System.out.println("File: " + f_Filename);      
      input_stream = record.value['fileRef'].getInputStream();
      input_stream.read(bytes);
      //byte[] bytes = ByteStreams.toByteArray(inputStream);
      input_stream.close();
      s = new String(bytes);

      lines = s.split('\n');
// ==== Parse the contents line by line
      def rIdx = 0;
      l = lines[rIdx];
          lf = l.split(',');
      
      while (lf[0].substring(0,3)!='900') {
System.out.println("Line " + rIdx.toString());
        
        if(lf[0] == '100') {
              f_VersionHeader = lf[1];
              LocalDateTime f_DateTime = LocalDateTime.parse(lf[2]+"00",DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
              f_FromParticipant = lf[3];
              f_ToParticipant = lf[4];
              
              newRecord = sdc.createRecord(record.sourceId + ':newRecordId');
              newMap = sdc.createMap(true);
              newMap['recType'] = 'header';
              newMap['filename'] = f_Filename;
              newMap['versionHeader'] = f_VersionHeader;
              newMap['fileDateTime'] = f_DateTime;
              newMap['fromParticipant'] = f_FromParticipant;
              newMap['toParticipant'] = f_ToParticipant;
              newRecord.value = newMap;
              //sdc.output.write(newRecord);
              recList.add(newRecord);
            rIdx = rIdx + 1;
          }

        if(lf[0] == '200') { // NEM12 file
          
            // Sample data
            // 200,NEM1202022,E1Q1B1K1,B1,B1,N1,02022,KWH,30,
            // 200,NEM1201002,E1E2,    E2,E2,N2,01002,KWH,30,
            // 200,NEM1210185,E1,,E1,N1,10185,WH,15,

          f_NMI = lf[1];
          f_ConnSiteConfiguration = lf[2];
              f_ConnRegisterID = lf[3];
              f_NMISuffix = lf[4];
              f_MDMDataStreamID = lf[5];
              f_MeterSerialNo = lf[6];
              f_UOM = lf[7];
              f_IntervalLength = Integer.valueOf(lf[8]);
              numPoints = (1440/f_IntervalLength).intValueExact();
              f_nextScheduledReadDate = lf[9];

          rIdx = rIdx + 1;
          l = lines[rIdx];
          lf = l.split(',');
          while(lf[0] != '200' && lf[0].substring(0,3) != '900') {
              if (lf[0] == '300') {
                                   LocalDate f_IntervalDate = LocalDate.parse(lf[1],DateTimeFormatter.BASIC_ISO_DATE);
                  
                  f_QualityMethod = lf[1 + numPoints + 1];
                  f_ReasonCode = lf[1 + numPoints + 2];
                  f_ReasonDesc = lf[1 + numPoints + 3];
                  f_UpdateDateTime = lf[1 + numPoints + 4];

                  if(f_QualityMethod == 'V' || (f_QualityMethod == 'A' && (f_ReasonCode=='79' || f_ReasonCode=='89' || f_ReasonCode=='61') )) {  //There are code-400 records following, that must be processed
                    rIdx = rIdx + 1;
                      c400 = lines[rIdx].split(',');
                    rCodes = Arrays.copyOf(rCodes, numPoints);
                    Arrays.fill(rCodes,'');
                      while(c400[0] == '400') {
                        for(int i = Integer.valueOf(c400[1]); i<= Integer.valueOf(c400[2]); i++) {
                            rCodes[i-1] = c400[3] + ',' + c400[4] + ',' + c400[5]; 
                        }
                        
                        rIdx = rIdx + 1;
                        c400 = lines[rIdx].split(',');
                      }
                    rIdx = rIdx - 1;
                  } // of checking for code 400
                                
                  for(int i = 0; i < numPoints; i ++) {
                      newRecord = sdc.createRecord(record.sourceId + ':newRecordId');
                      newMap = sdc.createMap(true);
                      newMap['recType'] = 'reading';
                      newMap['filename'] = f_Filename;
                      newMap['nmi'] = f_NMI;
                      newMap['meterSerialNo'] = f_MeterSerialNo;
                      newMap['registerID'] = f_ConnRegisterID;
                      newMap['nmiSuffix'] = f_NMISuffix;
                      newMap['streamID'] = f_MDMDataStreamID;
                      newMap['intervalDate'] = f_IntervalDate.atStartOfDay().plus(Duration.of(i*f_IntervalLength, ChronoUnit.MINUTES));
                      if(f_QualityMethod == 'V' || (f_QualityMethod == 'A' && (f_ReasonCode=='79' || f_ReasonCode=='89' || f_ReasonCode=='61') )) {
                          iC400 = rCodes[i].split(',');
                     System.out.println("Line 95 reached, size is " + rCodes[i]);                                           
                          newMap['qualityMethod'] = iC400[0];
                          newMap['reasonCode'] = iC400[1];
                          newMap['reasonDesc'] = iC400[2];
                      } else {
                          newMap['qualityMethod'] = f_QualityMethod;
                          newMap['reasonCode'] = f_ReasonCode;
                          newMap['reasonDesc'] = f_ReasonDesc;
                      }
                      
                      newMap['updateDateTime'] = f_UpdateDateTime;
                      newMap['uom'] = f_UOM;
                      newMap['measure'] = lf[1 + 1 + i];
                      newRecord.value = newMap;
                      //sdc.output.write(newRecord); 
                      recList.add(newRecord);
                  } // of going through event record

                rIdx = rIdx + 1;
                 l = lines[rIdx];
                 lf = l.split(',');     
              }
            
              if(lines[rIdx].split(',')[0] == '500'){
          System.out.println("GOT 500");      
                  lf = lines[rIdx].split(',');
                  newRecord = sdc.createRecord(record.sourceId + ':newRecordId');
                  newMap = sdc.createMap(true);
                  newMap['recType'] = 'nem12_transaction';
                  newMap['filename'] = f_Filename;
                  newMap['nmi'] = f_NMI;
                  newMap['meterSerialNo'] = f_MeterSerialNo;
                  newMap['registerID'] = f_ConnRegisterID;
                  newMap['nmiSuffix'] = f_NMISuffix;
                  newMap['transCode'] = lf[1];
                  newMap['retServiceOrder'] = lf[2];
                  newMap['readDateTime'] = lf[3];
                  newMap['indexRead'] = lf[4];
                  newRecord.value = newMap;
                  //sdc.output.write(newRecord);
                  recList.add(newRecord);
                
                  rIdx = rIdx + 1;
                  l = lines[rIdx];
                  lf = l.split(',');
        System.out.println("After 500 got " + lf[0]);        
                } // of checking for code 500


            
           } 
      }  // of 200-block processing
      
       if(lf[0] == '250') { //NEM13 file processing
         // Sample Data
         // 250,1234567890,1141,01,11,11,METSER66,E,000021.2,20031001103230,A,,,000534.5,20040201100
         //     030,E64,77,,343.5,kWh,20040509, 20040202125010,20040203000130

         f_NMI = lf[1];
          f_ConnSiteConfiguration = lf[2];    // NMIConfiguration
              f_ConnRegisterID = lf[3];       // RegisterID
              f_NMISuffix = lf[4];            //NMISuffix
              f_MDMDataStreamID = lf[5];      //MDMDataStreamIdentifier
              f_MeterSerialNo = lf[6];                            //MeterSerialNumber
              f_DirectionIndicator = lf[7];                                   //   DirectionIndicator
              f_PreviousRegisterRead = Float.valueOf(lf[8]);         // PreviousREgisterRead
         LocalDateTime f_PreviousRegisterReadDateTime = lf[9].trim().isEmpty() ? null : LocalDateTime.parse(lf[9].trim(),DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
              f_PreviousQualityMethod = lf[10];
              f_PreviousReasonCode = lf[11];
              f_PreviousReasonDescription = lf[12];
              f_CurrentRegisterRead = Float.valueOf(lf[13]);         // CurrentRegisterRead
              LocalDateTime f_CurrentRegisterReadDateTime = lf[14].trim().isEmpty() ? null : LocalDateTime.parse(lf[14].trim(),DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));         
              f_CurrentQualityMethod = lf[15];
              f_CurrentReasonCode = lf[16];
              f_CurrentReasonDescription = lf[17];
              f_Quantity = Float.valueOf(lf[18]);    // Quantity
              f_UOM = lf[19];    // UOM
              LocalDate f_NextScheduledReadDate = lf[20].trim().isEmpty() ? null :  LocalDate.parse(lf[20].trim(),DateTimeFormatter.BASIC_ISO_DATE);
              LocalDateTime f_UpdateReadDateTime = lf[21].trim().isEmpty() ? null : LocalDateTime.parse(lf[21].trim(),DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
         LocalDateTime f_MSATSLoadDateTime = lf[22].trim().isEmpty() ? null : LocalDateTime.parse(lf[22].trim(),DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

              newRecord = sdc.createRecord(record.sourceId + ':newRecordId');
                  newMap = sdc.createMap(true);
                  newMap['recType'] = 'accum_reading';
                  newMap['filename'] = f_Filename;
                  newMap['nmi'] = f_NMI;
                  newMap['connSiteConfig'] = f_ConnSiteConfiguration;
                  newMap['MDMDataStreamID'] = f_MDMDataStreamID;
                  newMap['meterSerialNo'] = f_MeterSerialNo;
                  newMap['registerID'] = f_ConnRegisterID;
                  newMap['nmiSuffix'] = f_NMISuffix;
                  newMap['directionIndicator'] = f_DirectionIndicator;
                  newMap['previousRegisterRead'] = f_PreviousRegisterRead;
                  newMap['previousRegisterReadDateTime'] = f_PreviousRegisterReadDateTime;
                  newMap['previousRegisterRead'] = f_PreviousRegisterRead;
                  newMap['previousQualityMethod'] = f_PreviousQualityMethod;
                  newMap['previousReasonCode'] = f_PreviousReasonCode;
                  newMap['previousReasonDescription'] = f_PreviousReasonDescription;
                  newMap['currentRegisterRead'] = f_CurrentRegisterRead;
                  newMap['currentRegisterReadDateTime'] = f_CurrentRegisterReadDateTime;
                  newMap['currentRegisterRead'] = f_CurrentRegisterRead;
                  newMap['currentQualityMethod'] = f_CurrentQualityMethod;
                  newMap['currentReasonCode'] = f_CurrentReasonCode;
                  newMap['currentReasonDescription'] = f_CurrentReasonDescription;
                  newMap['quantity'] = f_Quantity;
                  newMap['uom'] = f_UOM;
                  newMap['nextScheduledReadDate'] = f_NextScheduledReadDate;
                  newMap['updateReadDateTime'] = f_UpdateReadDateTime;
                  newMap['msatsLoadDateTime'] = f_MSATSLoadDateTime;
                                                                    
                  newRecord.value = newMap;
                  recList.add(newRecord);
           // end of processing 250-record
                                                                    
          rIdx = rIdx + 1;
          l = lines[rIdx];
          lf = l.split(',');
          while(lf[0] != '250' && lf[0].substring(0,3) != '900') {
              if (lf[0] == '550') {
                // Example 550,N,,A,
                                   
                  f_PreviousTransCode = lf[1];
                  f_PreviousRetServiceOrder = lf[2];                
                  f_CurrentTransCode = lf[3];
                  f_CurrentRetServiceOrder = lf[4];                

                  newRecord = sdc.createRecord(record.sourceId + ':newRecordId');
                  newMap = sdc.createMap(true);
                  newMap['recType'] = 'nem13_transaction';
                  newMap['filename'] = f_Filename;
                  newMap['nmi'] = f_NMI;
                  newMap['meterSerialNo'] = f_MeterSerialNo;
                  newMap['registerID'] = f_ConnRegisterID;
                  newMap['nmiSuffix'] = f_NMISuffix;
                  newMap['previousTransCode'] = lf[1];
                  newMap['previousRetServiceOrder'] = lf[2];
                  newMap['currentTransCode'] = lf[3];
                  newMap['currentRetServiceOrder'] = lf[4];
                  newRecord.value = newMap;
                  recList.add(newRecord);
                
                rIdx = rIdx + 1;
                 l = lines[rIdx];
                 lf = l.split(',');     
              }  
            
           }     // end of 550-block processing    
       }  // of 250-block processing
        
          l = lines[rIdx];
          lf = l.split(',');
      } // end of contents processing   
      
      for(rec in recList) {
          sdc.output.write(rec);
      }

    } catch (e) {
        // Write a record to the error pipeline
        sdc.log.error("File " + record.attributes['filename'] + " " + e.toString(), e)
        sdc.error.write(record, "File " + record.attributes['filename'] + " " + e.toString())
    }

}
