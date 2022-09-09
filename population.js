const { dhis2destination , dhis2source } = require('./axiosconfig');
const fs = require('fs');
const csv = require('csv-parser');

postData = async (data)=>{
    try {
        await dhis2destination.post(`/dataValues?${data}`)
    } catch (error) {
        console.log(error)
        console.log("=================================================================================================")
        console.log("exit in postData")
        console.log("=================================================================================================")
        process.exit(1)

    }
}

makeDataValues = (row)=> {
    const datavalues =[];
    const masculin ='oVYNP4fGnTo';
    const feminin='ksBi2JIApqW';
    
    datavalues.push(`pe=2014&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2014)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2015&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2015)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2016&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2016)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2017&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2017)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2018&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2018)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2019&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2019)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2020&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2020)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2021&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2021)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2022&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2022)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2023&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2023)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2024&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2024)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2025&ou=${row.uid}&de=${masculin}&value=${parseInt(row.M2025)}&comment=Import population sous prefecture`)


    datavalues.push(`pe=2014&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2014)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2015&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2015)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2016&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2016)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2017&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2017)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2018&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2018)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2019&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2019)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2020&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2020)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2021&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2021)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2022&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2022)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2023&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2023)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2024&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2024)}&comment=Import population sous prefecture`)
    datavalues.push(`pe=2025&ou=${row.uid}&de=${feminin}&value=${parseInt(row.F2025)}&comment=Import population sous prefecture`)
    
    return datavalues;
}



main = async ()=>{
    
    

    const datas = [];
    fs.createReadStream('./population.csv').pipe(csv()).on('data', (row) => {
        datas.push(row);
    }).on('end', async () => {
        try {
            
               for (let index = 0; index < datas.length; index++) {
                 const row = datas[index]
                 const datavalues = makeDataValues(row);
                 
                 for (let d = 0; d < datavalues.length; d++) {
                    const datavalue = datavalues[d];
                    await postData(datavalue);
                 }
                 
                 console.log(row.name + " OK");
               }
              
        } catch (error) {
            console.log(error)
        }
    });
}



main();