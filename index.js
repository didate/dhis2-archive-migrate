const { dhis2destination , dhis2source } = require('./axiosconfig');
const fs = require('fs');
const csv = require('csv-parser');

/* const de ='fFvNf25ygzR'
const coc ='tFdansXIMbw'
const optionsrc=['jWLFQL2QzEg:eeruCK8vkwY','jWLFQL2QzEg:C8CIw3tHsdO']

const optiondest=['pIjJC4ub3em','tFdansXIMbw'] */


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
    console.log("Enter in readData funtion")

    let resulat ;
    try {
       resulat = await dhis2source.get(`/analytics?dimension=pe:${periods}&dimension=${csv.optionArchive}&dimension=ou:LEVEL-5;Ky2CzFdfBuO&filter=dx:${csv.dataElementArchive}&skipMeta=true`)
       console.log("Read OK for ", periods, csv.dataElementArchive , csv.comment)
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
    console.log("Enter in postData function")

    try {
        await dhis2destination.post(`/dataValues?${data}`)
        console.log("OK ", data)
    } catch (error) {
        console.log(error)
        console.log("=================================================================================================")
        console.log("exit in postData")
        console.log("=================================================================================================")
        process.exit(1)

    }
}


migrate =async (periods, csv) =>{
    console.log("Enter in migrate funtion")
    try {
        const data = await readData(periods, csv);
    
        for (let index = 0; index < data.length; index++) {
            const d = data[index];
            if(d[3]!==""){
                const datavalue =`pe=${d[0]}&ou=${d[2]}&value=${parseInt(d[3])}&de=${csv.dataElementNouveau}&co=${csv.categorieOptionComboNouveau}&comment=${csv.comment}`
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
    fs.createReadStream('./pev.csv').pipe(csv()).on('data', (row) => {
        datas.push(row);
    }).on('end', async () => {


        try {
            for (let year = 2018; year <= 2021; year++) {
                const periods = months(year)
               for (let index = 0; index < datas.length; index++) {
                 const csv = datas[index]
                 migrate(periods, csv)  
               }
            }
        } catch (error) {
            console.log(error)
        }
    });
}



main();