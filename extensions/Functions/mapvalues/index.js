
const assignValues = {
    removeComplainData_A_Grade: () => {
        return ["LID: SCR", "LID:SCR,", "CHASSI:SCR", "CHASSI:SCR", "CHASSI: SCR,", "EDGE SCR", "REAR SCR", "CHASSI: SCUFFS MIN,", "CHASSI: SCUFFS MIN", "SCREEN: KBD IMPRINTS MIN,", "SCREEN: KBD IMPRINTS MIN", "SCREEN: SCR MIN,", "SCREEN: SCR MIN", "LID: SCUFFS MIN", "LID: SCUFFS MIN,", "WORN:KBD MIN,", "WORN:KBD MIN", "WORN:PAD MIN", "WORN:PAD MIN,", "SCREEN: PRESS MARK MIN,", "SCREEN: PRESS MARK MIN", "CHASSI: SCR MIN,", "CHASSI: SCR MIN"];
    },
    removeprocessorData: () => {
        return ["INTEL(R) PENTIUM(R) CPU ", "FAMILY: CORE I5; VERSION: ", "FAMILY: CORE I7; VERSION: ", "INTEL(R) XEON(R) CPU ", "INTEL(R) CORE(TM) ", "AMD ", "PROCESSOR ", "INTEL(R) XEON(R) CPU", "INTEL(R) CORE(TM)", "AMD", "PROCESSOR", "INTEL(R) XEON(R) CPU;", "INTEL(R) CORE(TM);", "AMD;", "PROCESSOR;", "11TH GEN ", "12TH GEN ", "13TH GEN ", "14TH GEN ", "15TH GEN ", "INTEL(R) XEON(R) ", "INTEL(R) CELERON(R) CPU ", "FAMILY: CELERON; VERSION: ", "INTEL(R) CELERON(R) ", "INTEL; FAMILY: XEON; VERSION: ", "INTEL(R) ATOM(TM) CPU ", "PENTIUM(R) DUAL-CORE CPU ", "DUAL-CORE CPU "];
    },
    removeModelData: () => {
        return ["NOTEBOOK PC", "15.6 INCH", "14 INCH", "MOBILE WORKSTATION PC", "WITH RADEON GRAPHICS", "HP Compaq ", "HP ", "Dell ", "Lenovo ", "Compaq ", "HP Compaq", "HP", "Dell", "Lenovo", "Compaq"];
    },
    removeComplainData: () => {
        return ["SCREEN MIN SCR", "REAR MIN SCR", "EDGE MIN SCR", "chassi: glue", "chassi:glue", "chassi glue", "lid: glue", "Chassi min scr", "pad.worn", "rem BIOS pwd", "lid: min scr", "Reset to factory defaults", "Chassi.Glue", "Powerwashed", "lid; min scr", "chassi; min scr", "kbd; no TP", "pad; worn", "Factory reset", "HW", "POS unit", "POS unit,", "kbd; worn ", "kbd; worn", "Missing adapter", "pad:worn", "lid; min scr", "chassi: min scr", "lid, min scr", "pad, worn", "KBD: worn", "chassii: min scr", "chassi: min scr", "NO TP", "Missing TP", "pad: worn", "chassi. min scr", "MDM UNLOCKED", "REM MDM LOCK", "rem bios pwd,", "rem bios pwd;", "BIOS PW REM MIN", "BIOS PW REM", "REM COMPUTRACE", ",BIOS PW REM MIN", ",BIOS PW REM", ",REM COMPUTRACE", 'min', ' min', 'MIN', ' MIN'];
    },
    ASSIGNSTOCKLISTVALUES: async (data, type,api_name) => {
        let rows = []
        data.forEach(async (item) => {
            item.target_price = item.target_price ? Math.round(item.target_price) : '';
            assignValues.removeModelData().forEach((removable) => {
                if (item.model) {
                    item.model = item.model.toUpperCase().replace(removable, "").trim();
                    return item.model;
                }
            })
            assignValues.removeprocessorData().forEach((removable) => {
                if (item.processor) {
                    item.processor = item.processor.toUpperCase().replace(removable, "").trim();
                    return item.processor;
                }
            })
            if (item.asset_type === 'COMPUTER PARTS') {
                item.asset_type = 'PARTS COMPUTER'
            }
            if (item.asset_type === 'SERVER PARTS') {
                item.asset_type = 'PARTS SERVER'
            }
            if (item.battery && item.form_factor && item.form_factor.toLowerCase() === 'laptop') {
                let temp = item.battery.split(':')
                if (temp[1]?.includes('%')) {
                    temp.forEach((obj) => {
                        if (obj.includes('%')) {
                            let value = obj.split('%')[0] ? Math.round(obj.split('%')[0].trim()) : null
                            if (value < 50) {
                                item.battery = "def/low % battery";
                                item.complaint = "def bat";
                            } else {
                                item.battery = ""
                            }
                        }
                    })
                }
            }
            if (item?.complaint_from_app && !item?.complaint) {
                item.complaint = item?.complaint_from_app
            }
            if (item.complaint) {
                item.complaint = item.complaint.replace(/;/g, " ");
            }
            if (item.complaint_from_app) {
                item.complaint_from_app = item.complaint_from_app.replace(/;/g, " ");
            }

            assignValues.removeComplainData().forEach((removable) => {
                if (item.complaint) {
                    item.complaint = item.complaint.toUpperCase().replace(removable.toUpperCase(), "").trim();
                    return item.complaint;
                }
            })
            assignValues.removeComplainData_A_Grade().forEach((removable) => {
                if (item.grade === "A" && item.complaint) {
                    item.complaint = item.complaint.toUpperCase().replace(removable.toUpperCase(), "").trim();
                    return item.complaint;
                }
            })
            if (item.complaint) {
                //----------- Delete first character of string if it is comma from string if its firts
                while (item.complaint.charAt(0) === ',') {
                    item.complaint = item.complaint.substring(1).trim();
                }
                //-------------
                let complaint = item.complaint.replace(',', "").trim();
                if (complaint.toUpperCase() === 'MIN') {
                    item.complaint = ''; // set cmplaint as empty when value shows only MIN
                }
            }


            if (item.asset_type && (item.asset_type.toUpperCase() === 'COMPUTER')) {
                let hddtext = ["no hdd", "hdd rem", "hdd crash", "hdd fail"]
                let complaint = item?.complaint ? item.complaint.toLowerCase() : null
                let complaint_1 = item?.complaint_1 ? item.complaint_1.toLowerCase() : null;
                let isComplaintTrue = false;
                let isComplaint_1True = false;
                hddtext.forEach((obj) => {
                    if (complaint && complaint.includes(obj)) {
                        isComplaintTrue = true;
                    }
                    if (complaint_1 && complaint_1.includes(obj)) {
                        isComplaint_1True = true;
                    }
                })
                if (item.hdd && (item.hdd !== 'N/A' && !item.hdd.includes('/') && (isComplaintTrue || isComplaint_1True))) {
                    item.hdd = 'N/A'
                }
                else if (!item.hdd && (isComplaintTrue || isComplaint_1True)) {
                    item.hdd = 'N/A'
                }
            }
            if (type) {
                if (item.asset_type === 'COMPUTER' && item.asset_type === 'MOBILE DEVICE') {
                    if ((item.data_destruction === '' || item.data_destruction === null || item.data_destruction === 'ERASED')
                        && item.complaint.includes("HDD REM") && item.hdd === 'N/A') {
                        rows.push(item)
                    }
                } else {
                    rows.push(item)
                }
            } else {
                rows.push(item)
            }
        });
        let stocks_report = [];
        rows.forEach((item) => {
            if (item.asset_type === 'COMPUTER' || item.asset_type === 'SERVER & STORAGE' || item.asset_type.toLowerCase().includes("mobile") || item.asset_type === 'COMPUTER') {
                if (item.data_destruction) {
                    let dataDestruction = item.data_destruction.toLowerCase();
                    if (
                        dataDestruction === "erasure in progress"
                        || dataDestruction === "not erased/not erased/not erased"
                        || dataDestruction === "not erased/not erased"
                        || dataDestruction === "failed sectors"
                        || dataDestruction.includes("erased with warning(s) (reallocated sectors not erased: ")
                        || dataDestruction.includes("erased with warnings (reallocated sectors not erased: ")
                        || dataDestruction.includes("not erased (")
                        || dataDestruction.toLowerCase() === "not erased") {
                        delete item
                    }else {
                    stocks_report.push(item)
                    }
                } else {
                    if(api_name){
                        delete item.data_destruction
                    }
                    stocks_report.push(item)
                }
            } else {
                                  if(api_name){
                        delete item.data_destruction
                    }
                stocks_report.push(item)
            }
        })
        return stocks_report
    }

}

module.exports = assignValues;