const { dhis2destination , dhis2source } = require('./axiosconfig');
const fs = require('fs');
const csv = require('csv-parser');


months = (year) =>{
    let monthString ="";
    for (let month = 1; month <=12; month++) {
        if(month===1){
            monthString +=`${year}0${month}`
        }else{
            monthString += month <10 ? `;${year}0${month}` : `;${year}${month}`    
        }
    }
    return monthString;
}


readData = async (periods, csv)=>{

    


    let resulat ;
    try {
        let dimensionOptionArchive =""
        if(csv.optionArchive !==""){
            dimensionOptionArchive =`&dimension=${csv.optionArchive}`
        }
       resulat = await dhis2source.get(`/analytics?dimension=pe:${periods}&dimension=ou:LEVEL-5;Ky2CzFdfBuO${dimensionOptionArchive}&filter=dx:${csv.dataElementArchive}&skipMeta=true`)
       console.log(periods + ' READ FROM ARCHIVE');
    } catch (error) {
        console.log(error)
        console.log("=================================================================================================")
        console.log("exit in readData")
        console.log("=================================================================================================")
        process.exit(1)

    }
    return resulat && resulat.data ? resulat.data.rows : [];

}

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


migrate =async (periods, csv) =>{
    try {
        const data = await readData(periods, csv);
    
        for (let index = 0; index < data.length; index++) {
            const d = data[index];
           
            let datavalue = ""
            const value = csv.optionArchive ==="" ? d[2] : d[3]
            if(value!==""){
                
                let categorieOptionComboNouveau =""

                if(csv.categorieOptionComboNouveau !==""){
                    categorieOptionComboNouveau =`&co=${csv.categorieOptionComboNouveau}`
                }
                datavalue =`pe=${d[0]}&ou=${d[1]}&value=${parseInt(value)}&de=${csv.dataElementNouveau}${categorieOptionComboNouveau}&comment=${csv.comment}`
                await postData(datavalue)
            }
            
        }
    } catch (error) {
        console.log(error)
        console.log("=================================================================================================")
        console.log("exit in migrate")
        console.log("=================================================================================================")
        process.exit(1)

    }
}


main = async ()=>{
    

    const datas = [];
    fs.createReadStream('./pev2016.csv').pipe(csv()).on('data', (row) => {
        datas.push(row);
    }).on('end', async () => {


        try {
            for (let year = 2016; year <= 2017; year++) {
                console.log(year + ' STARTING ....');
                const periods = months(year)
               for (let index = 0; index < datas.length; index++) {
                 const csv = datas[index]
                 await migrate(periods, csv)  
               }
               console.log(year + ' OK ....');
            }
        } catch (error) {
            console.log(error)
        }
    });
}



main();