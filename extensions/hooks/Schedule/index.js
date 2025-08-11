const { CRONJOBS } = require('../../Functions');
const { removeprocessorData } = require('../../Functions/mapvalues');

module.exports = async function registerHook(hooktypes, app) {
    let request = require('request')
    let _ = require('underscore')
    let moment = require('moment')
    let offset = 0;
    // console.log("hooktypes", hooktypes)
    const database = app.database;
    // const env = app.env;
    const ItemsService = app.services.ItemsService;
    // const MailService = app.services.MailService;
    const schema = await app.getSchema();
    const cron = require("node-cron");

    const assetsService = new ItemsService('Assets', {
        schema
    });

    const certusService = new ItemsService('Certus', {
        schema
    });

    const certusMobileService = new ItemsService('CertusMobileNew', {
        schema
    });


    const ServiceUnavailableException = app.exceptions.ServiceUnavailableException;


    cron.schedule('*/1 * * * *', async () => {
        let sql = `update public."Assets" set asset_id_1= asset_id where asset_id_1 is null`;
        await database.raw(sql)
            .then(async (response) => {
            })
            .catch((error) => {
                // res.send(500)
            });


        let sql1 = `update public."Assets" set project_id_1 = project_id  where project_id_1 is null and project_id is not null`;
        await database.raw(sql1)
            .then(async (response) => {
            })
            .catch((error) => {
                // res.send(500)
            });
    })
    //-----------------------------------
    // cron.schedule('*/35 * * * *', async () => {
    //     try {
    //         request(certusOptionsEq(0, 'eq'), async function (error, response1) {
    //             if (error) {
    //                 // res.status("500").send(error);
    //                 console.log("error certus", error)
    //             } else {
    //                 // return
    //                 console.log("certus data === >", JSON.parse(response1.body).length)
    //                 if (response1.body && JSON.parse(response1.body).length > 0) {
    //                     offset += JSON.parse(response1.body).length;

    //                     await insertCertusData(response1.body, 1000)
    //                 }
    //             }
    //         });
    //     } catch (err) {
    //         throw new ServiceUnavailableException(error);
    //     }
    // })



    // cron.schedule('0 */1 * * *', async () => {
    //     fetchRecordsComputer()
    // })

    // cron.schedule('0 */2 * * *', async () => {
    //     fetchRecordsMobile()
    // })
    // fetchRecordsComputer()
    // fetchRecordsMobile()
    // request(certusOptionsEq(0, 'eq'), async function (error, response1) {
    //     if (error) {
    //         // res.status("500").send(error);
    //         console.log("error certus", error)
    //     } else {
    //         // return
    //         console.log("certus data === >", JSON.parse(response1.body).length)
    //         if (response1.body && JSON.parse(response1.body).length > 0) {
    //             offset += JSON.parse(response1.body).length;

    //             await insertCertusData(response1.body)
    //         }
    //     }
    // });


    // let arr1 = [245296, 245251, 245298, 245532, 245497, 245493, 245488, 245491, 245948, 245952, 245862, 245918, 250425, 250426, 250417, 245908, 245906, 245910, 245829, 245832, 250392, 245850, 245899, 245565, 249717, 249718, 245571, 245572, 245554, 249091, 245798, 245342, 249048, 249195, 249198, 249200, 249196, 249197, 245716, 249164, 245534, 245468, 238045, 238043, 245415, 245408, 244707, 248365, 248366, 244191, 244192, 244193, 239955];
    // arr1.forEach((itm) => {
    //     console.log("aaaaaaaaa", itm)
    //     request(certusOptionsEq(itm), async function (error, response1) {
    //         if (error) {
    //             // res.status("500").send(error);
    //             console.log("error certus", error)
    //         } else {
    //             // return
    //             // console.log("certus data === >", JSON.parse(response1.body).length)
    //             if (response1.body && JSON.parse(response1.body).length > 0) {
    //                 offset += JSON.parse(response1.body).length;

    //                 await insertCertusData(response1.body, 10000)
    //             }
    //         }
    //     });
    // })

    // // cron.schedule('*/1 * * * *', async () => {
    // //     request(certusOptionsEq(0, 'eq'), async function (error, response1) {
    // //         if (error) {
    // //             // res.status("500").send(error);
    // //             console.log("error certus")
    // //         } else {

    // //             if (response1.body && JSON.parse(response1.body).length > 0) {

    // //                 offset += JSON.parse(response1.body).length;
    // //                 // console.log(offset, "response.body 111", JSON.parse(response1.body).length)
    // //                 await insertAssets(response1.body)
    // //             }
    // //         }
    // //     });

    // // })


    // cron.schedule('*/55 * * * *', async () => {
    //     try {
    //         request(certusOptionsMobileEq(0, 'eq'), async function (error, response1) {
    //             if (error) {
    //                 // res.status("500").send(error);
    //                 console.log("eroororr certus", error)
    //             } else {
    //                 // await CRONJOBS(cronjobsservice, (response1.body), 'certusmobile')
    //                 if (response1.body && JSON.parse(response1.body).length > 0) {
    //                     offset += JSON.parse(response1.body).length;
    //                     await insertMobileCertusData(response1.body, 10000)
    //                 }
    //             }
    //         });
    //     } catch (err) {
    //         throw new ServiceUnavailableException(error);
    //     }

    // })
    //fetchRecordsComputer()

    async function fetchRecordsComputer() {
        try {
            const oneMonthAgo = new Date();
            oneMonthAgo.setDate(oneMonthAgo.getDate() - 10);
            // const isoDate = oneMonthAgo.toISOString();
            // console.log("isoDate",isoDate)
            // const assets = [250714, 250715, 250716, 250717, 250718, 250719, 250720, 250721, 250722, 250723, 250724, 250725, 250726, 250727, 250728, 250729, 250730, 250731, 250732, 250733, 250734, 250735, 250736, 250737, 250738, 250739, 250740, 250741, 250742, 250743, 250744, 250745, 250746, 250747, 250748, 250749, 250750, 250751, 250752, 250753, 250754, 250755, 250756, 250757, 250758, 250759, 250760, 250761, 250762, 250763, 250764, 250765, 250766, 250767, 250768, 250769, 250770, 250771, 250772, 250773, 250774, 250775, 250776, 250777, 250778, 250779, 250780, 250781, 250782, 250783, 250784, 250785, 250786, 250787, 250789, 250791, 250792, 250794, 250795, 250796, 250797, 250798, 250799, 250800, 250801, 250802, 250803, 250804, 250805, 250806, 250807, 250808, 250809, 250810, 250811, 250812, 250813, 250815, 250816, 250817, 250818, 250819, 250820, 250821, 250822, 250823, 250824, 250825, 250826, 250827, 250828, 250829, 250830, 250831, 250832, 250833, 250834, 250835, 250836, 250837, 250838, 250839, 250840, 250841, 250842, 250843, 250844, 250845, 250846, 250847, 250848, 250849, 250850, 250851, 250853, 250854, 250855, 250856, 250974, 250975, 250976, 250977, 250978, 250979, 250980, 250981, 250982, 250983, 250984, 250985, 250986, 250987, 250988, 250989, 250990, 250991, 250992, 250993, 250994, 250995, 250996, 250997, 250998, 250999, 251000, 251001, 251002, 251003, 251004, 251005, 251006, 251007, 251008, 251009, 251010, 251011, 251012, 251013, 251014, 251015, 251016, 251017, 251018, 251019, 251020, 251021, 251022, 251023, 251024, 251025, 251026, 251027, 251028, 251029, 251030, 251031, 251032, 251033, 251034, 251035, 251036, 251037, 251038, 251039, 251040, 251041, 251042, 251043, 251045, 251046, 251047, 251048, 251049, 251050, 251051, 251052, 251053, 251054, 251055, 251056, 251057, 251058, 251059, 251060, 251061, 251062, 251063, 251064, 251065, 251066, 251067, 251068, 251069, 251070, 251071, 251072, 251073, 251120, 251121, 251122, 251123, 251124, 251125, 251126, 251127, 251128, 251129, 251130, 251131, 251132, 251133, 251134, 251135, 251136, 251137, 251138, 251139, 251140, 251141, 251142, 251143, 251144, 251145, 251146, 251147, 251148, 251149, 251150, 251151, 251152, 251154, 251155, 251156, 251157, 251158, 251159, 251161, 251162, 251163, 251164, 251165, 251166, 251167, 251168, 251169, 251170, 251171, 251172, 251173, 251174, 251175, 251176, 251177, 251178, 251179, 251180, 251181, 251182, 251183, 251184, 251185, 251186, 251187, 251188, 251189, 251190, 251191, 251192, 251193, 251194, 251195, 251196, 251197, 251198, 251199, 251200, 251201, 251202, 251203, 251204, 251205, 251206, 251207, 251208, 251209, 251210, 251211, 251212, 251213, 251214, 251215, 251216, 251217, 251218, 251219, 251220, 251221, 251222, 251223, 251224, 251225, 251226, 251227, 251228, 251229, 251230, 251231, 251232, 251233, 251234, 251235, 251236, 251237, 251238, 251240, 251241, 251242, 251243, 251244, 251245, 251246, 251247, 251248, 251249, 251250, 251251, 251252, 251253, 251254, 251255, 251256, 251257, 251258, 251259, 251260, 251261, 251262, 251263, 251264, 251265, 251266, 251267, 251268, 251269, 251270, 251271, 251272, 251273, 251274, 251275, 251276, 251277, 251278, 251279, 251280, 251281, 251282, 251283, 251284, 251285, 251286, 251287, 251288, 251289, 251290, 251291, 251292, 251293, 251294, 251295, 251296, 251298, 251299, 251300, 251301, 251302, 251303, 251304, 251305, 251306, 251307, 251308, 251309, 251310, 251311, 251312, 251313, 251314, 251315, 251316, 251317, 251318, 251319, 251329, 251330, 251331, 251332, 251333, 251334, 251335, 251336, 251337, 251338, 251339, 251340, 251341, 251342, 251343, 251344, 251345, 251346, 251347, 251348, 251349, 251350, 251351, 251352, 251353, 251354, 251355, 251356, 251358, 251359, 251360, 251361, 251362, 251363, 251364, 251365, 251366, 251367, 251368, 251369, 251370, 251371, 251372, 251373, 251374, 251375, 251376, 251377, 251378, 251379, 251380, 251381, 251382, 251383, 251384, 251385, 251386, 251387, 251388, 251389, 251390, 251452, 252037, 252038, 252039, 252040, 252041, 252042, 252043, 252044, 252045, 252046, 252047, 252048, 252049, 252050, 252051, 252052, 252053, 252054, 252055, 252056, 252057, 252058, 252059, 252060]
            const assetLists = await assetsService.readByQuery({
                fields: ["asset_id", "asset_type", "grade", "project_id", "date_created"],
                limit: -1,
                filter: {
                    _and: [
                        {
                            platform: {
                                _icontains: 'MOBILE_UPDATE'
                            },
                        },
                        {
                            _or: [
                                { asset_type: { _null: true } },
                                //{ processor: { _null: true } },
                            ]
                        }
                    ]
                },
                sort: ['-date_created'],

            });
            // console.log("assetLists?.length", assetLists?.length)
            if (assetLists?.length > 0) {
                for (const obj of assetLists) {
                    console.log("obj.asset_id", obj.asset_id)
                    request(certusOptionsEq_1(obj.asset_id), async function (error, response1) {
                        if (error) {
                            // res.status("500").send(error);
                            console.log(obj.asset_id, "eroororr certus", error)
                        } else {
                            // console.log(obj.asset_id, "computer certus response", JSON.parse(response1.body).length)

                            // await CRONJOBS(cronjobsservice, (response1.body), 'certusmobile')
                            if (response1.body && JSON.parse(response1.body).length > 0) {
                                offset += JSON.parse(response1.body).length;
                                await insertCertusData(response1.body, 10000)
                            } else {
                                // console.log("mobile elsee=====> ")
                                request(certusOptionsMobileEq_1(obj.asset_id), async function (error, response1) {
                                    if (error) {
                                        // res.status("500").send(error);
                                        console.log(obj.asset_id, "eroororr certus", error)
                                    } else {
                                        // console.log(obj.asset_id, "mobile certus response", JSON.parse(response1.body).length)

                                        // await CRONJOBS(cronjobsservice, (response1.body), 'certusmobile')
                                        if (response1.body && JSON.parse(response1.body).length > 0) {
                                            offset += JSON.parse(response1.body).length;
                                            await insertCertusData(response1.body, 10000)
                                        }
                                    }
                                });
                            }
                        }
                    });
                }
            }
        } catch (err) {
            throw err
        }
    }
    // let arr = [
    //     241281,
    //     241284,
    //     241262,
    //     241268,
    //     241272,
    //     241288,
    //     241293,
    //     241306,
    //     241305,
    //     241292
    //     ];
    // arr.forEach((itm) => {
    //     request(certusOptionsMobileEq(itm), async function (error, response1) {
    //         if (error) {
    //             // res.status("500").send(error);
    //             console.log("eroororr certus", error)
    //         } else {
    //             console.log("JSON.parse(response1.body).length", JSON.parse(response1.body).length)

    //             // await CRONJOBS(cronjobsservice, (response1.body), 'certusmobile')
    //             if (response1.body && JSON.parse(response1.body).length > 0) {
    //                 offset += JSON.parse(response1.body).length;
    //                 await insertMobileCertusData(response1.body, 10000)
    //             }
    //         }
    //     });
    // })


    const date = moment();
    let dateTo = date.format('YYYY-MM-DD');
    let dateFrom = date.subtract(4, 'd').format('YYYY-MM-DD');
    let dateFrom1 = moment(dateFrom).subtract(4, 'd').format('YYYY-MM-DD');
    let dateFrom2 = moment(dateFrom1).subtract(4, 'd').format('YYYY-MM-DD');
    let dateFrom3 = moment(dateFrom2).subtract(4, 'd').format('YYYY-MM-DD');

    // cron.schedule("*/46 * * * *", async (req, res) => {
    //     fetchComputerCertusRangeValues(dateTo, dateFrom, 1500)
    // });

    // cron.schedule("*/4 * * * *", async (req, res) => {
    //     fetchComputerCertusRangeValues(dateFrom, dateFrom1, 5000)
    // });

    // cron.schedule("*/65 * * * *", async (req, res) => {
    //     fetchComputerCertusRangeValues(dateFrom1, dateFrom2)
    // });

    // cron.schedule("*/75 * * * *", async (req, res) => {
    //     fetchComputerCertusRangeValues(dateFrom2, dateFrom3)
    // });

    async function fetchComputerCertusRangeValues(dateTo, dateFrom, limit) {
        request(certusOptionsRange(dateTo, dateFrom), async function (error, response1) {
            if (error) {
                res.status("500").send(error);
            } else {
                // await CRONJOBS(cronjobsservice, (response1.body), 'certuscomputerrange')
                console.log("JSON.parse(response1.body).length", JSON.parse(response1.body).length)
                if (response1.body && JSON.parse(response1.body).length > 0) {
                    await insertCertusData(response1.body, limit)
                }
            }
        });
    }

    // cron.schedule("*/59 * * * *", async (req, res) => {
    //     fetchMobileCertusRangeValues(dateTo, dateFrom1)
    // });

    // cron.schedule("*/4 * * * *", async (req, res) => {
    //     fetchMobileCertusRangeValues(dateFrom, dateFrom1)
    // });

    // cron.schedule("*/6 * * * *", async (req, res) => {
    //     fetchMobileCertusRangeValues(dateFrom1, dateFrom2)
    // });

    // cron.schedule("*/115 * * * *", async (req, res) => {
    //     fetchMobileCertusRangeValues(dateFrom1, dateFrom2)
    // });

    async function fetchMobileCertusRangeValues(dateTo, dateFrom) {
        request(certusOptionsMobileRange(dateTo, dateFrom), async function (error, response1) {
            if (error) {
                res.status("500").send(error);
            } else {
                // await CRONJOBS(cronjobsservice, (response1.body), 'certusmobilerange')

                if (response1.body && JSON.parse(response1.body).length > 0) {
                    await insertMobileCertusData(response1.body)
                }
            }
        });
    }


    // cron.schedule('*/20 * * * *', async () => {
    //     let sql1 = `select asset_id from public."Assets" where grade_from_app is not null and model is null`
    //     await database.raw(sql1)
    //         .then(async (response) => {
    //             let result = response.rows;
    //             console.log("result.length", result.length);
    //             for (let i = 0; i <= result.length; i++) {
    //                 await delay();
    //                 let url = `https://cloud.certus.software/webservices/rest-api/v1/reports/ce`;
    //                 request.post({
    //                     url: url,
    //                     headers: {
    //                         "Customer-Code": "220811",
    //                         Authorization: "Basic dG9yZC5oZW5yeXNvbjpJdG9ub215OEA=",
    //                         "content-type": "application/json",
    //                     },
    //                     body: `{
    //                         "reportMode": "ORIGINAL",
    //                         "groupData": "DRIVE",
    //                         "request": {
    //                           "filter": {
    //                             "criteria": [
    //                                 {
    //                                     "column": "cewm.ce.report.document.custom.field5",
    //                                         "conditions": [{
    //                                             "type": "text",
    //                                             "operator": "eq",
    //                                             "value": "${result[i].asset_id}"
    //                                         }
    //                                     ]
    //                                 }
    //                             ]
    //                           }
    //                         },
    //                       "response": {
    //                   "showColumns": ["cewm.ce.report.hardware.system.version","cewm.ce.report.hardware.system.family","cewm.ce.report.erasure.status.warning","cewm.ce.report.document.id", "cewm.ce.report.date", "cewm.ce.report.document.software.version", "cewm.ce.report.document.custom.field1", "cewm.ce.report.document.custom.field2", "cewm.ce.report.document.custom.field3", "cewm.ce.report.document.custom.field4", "cewm.ce.report.document.custom.field5", "cewm.ce.report.document.operator.name", "cewm.ce.report.document.operator.group.name", "cewm.ce.report.hardware.lot.name", "cewm.ce.report.hardware.system.manufacturer", "cewm.ce.report.hardware.system.serial.number", "cewm.ce.report.hardware.system.chassis.type", "cewm.ce.report.hardware.system.model", "cewm.ce.report.hardware.system.uuid", "cewm.ce.report.hardware.system.motherboard", "cewm.ce.report.hardware.system.bios", "cewm.ce.report.hardware.system.processor", "cewm.ce.report.hardware.system.device", "cewm.ce.report.hardware.system.memory", "cewm.ce.report.hardware.system.graphic.card", "cewm.ce.report.hardware.system.sound.card", "cewm.ce.report.hardware.system.network.adapter", "cewm.ce.report.hardware.system.optical.drive", "cewm.ce.report.hardware.system.storage.controller", "cewm.ce.report.hardware.system.peripheral.ports", "cewm.ce.report.hardware.system.battery", "cewm.ce.report.erasure.lot.name", "cewm.ce.report.erasure.device.vendor", "cewm.ce.report.erasure.device.model", "cewm.ce.report.erasure.device.type", "cewm.ce.report.erasure.device.bus.type", "cewm.ce.report.erasure.device.serial.number", "cewm.ce.report.erasure.device.size", "cewm.ce.report.erasure.device.sectors", "cewm.ce.report.erasure.device.sector.size", "cewm.ce.report.erasure.device.remapped.sectors", "cewm.ce.report.erasure.device.hpa", "cewm.ce.report.erasure.device.dco", "cewm.ce.report.erasure.pattern", "cewm.ce.report.erasure.verification.percentage", "cewm.ce.report.erasure.time.start", "cewm.ce.report.erasure.time.end", "cewm.ce.report.erasure.duration", "cewm.ce.report.erasure.status", "cewm.ce.report.erasure.hidden.areas", "cewm.ce.report.erasure.sectors", "cewm.ce.report.erasure.failed.sectors", "cewm.ce.report.erasure.remapped.sectors", "cewm.ce.report.erasure.software.version", "cewm.ce.report.erasure.compliance.requested", "cewm.ce.report.erasure.compliance.resulted", "cewm.ce.report.erasure.smart.health", "cewm.ce.report.erasure.smart.performance", "cewm.ce.report.erasure.smart.erl", "cewm.ce.report.erasure.smart.power.on.time", "cewm.ce.report.erasure.smart.read.errors", "cewm.ce.report.erasure.smart.read.errors.corrected", "cewm.ce.report.erasure.smart.read.errors.uncorrected", "cewm.ce.report.erasure.smart.write.errors", "cewm.ce.report.erasure.smart.write.errors.corrected", "cewm.ce.report.erasure.smart.write.errors.uncorrected", "cewm.ce.report.erasure.smart.verify.errors", "cewm.ce.report.erasure.smart.verify.errors.corrected", "cewm.ce.report.erasure.smart.verify.errors.uncorrected", "cewm.ce.report.status"]
    //                             }
    //                       }`,
    //                 },
    //                     async function (error, response) {
    //                         if (error) {
    //                             // res.status("500").send(error);
    //                             console.log("eroororr certus", error);

    //                         } else {
    //                             if (response.body && JSON.parse(response.body).length > 0) {
    //                                 await insertCertusData(response.body)

    //                             }
    //                         }

    //                     }
    //                 );
    //             }
    //         })
    //         .catch((error) => {

    //         });
    // })

    // cron.schedule("*/1 * * * *", async (req, res) => {
    // let sql1 = `select asset_id,processor, date_created, created_at,status from public."Assets" where UPPER(processor) like '%GHZ%' and UPPER(processor) like 'I%' and UPPER(processor) not like 'IN%' order by created_at desc`
    // await database.raw(sql1)
    //     .then(async (response) => {
    //         let result = response.rows;
    //         for (let i = 0; i <= result.length; i++) {
    //             let processor = result[i].processor.split(" ");
    //             if (processor[0]) {
    //                 console.log(result[i].asset_id,"processor=>", processor[0])

    //                 await assetsService.updateOne(result[i].asset_id,
    //                     {
    //                         processor: processor[0],
    //                         asset_id: result[i].asset_id
    //                     }
    //                 )
    //             }
    //         }
    //     })
    //     .catch((error) => {

    //     });
    // })



    function certusOptionsRange(dateTo, dateFrom) {
        // const date = moment();
        // let dateTo = date.format('YYYY-MM-DD');
        // let dateFrom = date.subtract(5, 'd').format('YYYY-MM-DD');
        // console.log("date TO", dateTo)
        // console.log("date From", dateFrom)
        return {
            method: "POST",
            url: "https://cloud.certus.software/webservices/rest-api/v1/reports/ce",
            headers: {
                "Customer-Code": "220811",
                Authorization: "Basic dG9yZC5oZW5yeXNvbjpJdG9ub215OEA=",
                "Content-Type": "application/json",
            },
            body: `{
            "reportMode": "ORIGINAL",
            "groupData": "DRIVE",
            "request": {
              "filter": {
                "criteria": [{
                    "column": "cewm.ce.report.erasure.time.end",
                    "conditions": [{
                        "type": "date",
                        "operator": "inRange",
                        "date": ${dateFrom},
                        "dateTo": ${dateTo}
                    }]
                }],
                "conjunction": "AND"
              },
              "sort": [{
                "column": "cewm.ce.report.erasure.time.end",
                "mode": "ASC"
              }],
              "limit": 1000,
              "offset": 0
            },
          "response": {
                  "showColumns": ["cewm.ce.report.hardware.system.version","cewm.ce.report.hardware.system.family","cewm.ce.report.erasure.status.warning","cewm.ce.report.document.id", "cewm.ce.report.date", "cewm.ce.report.document.software.version", "cewm.ce.report.document.custom.field1", "cewm.ce.report.document.custom.field2", "cewm.ce.report.document.custom.field3", "cewm.ce.report.document.custom.field4", "cewm.ce.report.document.custom.field5", "cewm.ce.report.document.operator.name", "cewm.ce.report.document.operator.group.name", "cewm.ce.report.hardware.lot.name", "cewm.ce.report.hardware.system.manufacturer", "cewm.ce.report.hardware.system.serial.number", "cewm.ce.report.hardware.system.chassis.type", "cewm.ce.report.hardware.system.model", "cewm.ce.report.hardware.system.uuid", "cewm.ce.report.hardware.system.motherboard", "cewm.ce.report.hardware.system.bios", "cewm.ce.report.hardware.system.processor", "cewm.ce.report.hardware.system.device", "cewm.ce.report.hardware.system.memory", "cewm.ce.report.hardware.system.graphic.card", "cewm.ce.report.hardware.system.sound.card", "cewm.ce.report.hardware.system.network.adapter", "cewm.ce.report.hardware.system.optical.drive", "cewm.ce.report.hardware.system.storage.controller", "cewm.ce.report.hardware.system.peripheral.ports", "cewm.ce.report.hardware.system.battery", "cewm.ce.report.erasure.lot.name", "cewm.ce.report.erasure.device.vendor", "cewm.ce.report.erasure.device.model", "cewm.ce.report.erasure.device.type", "cewm.ce.report.erasure.device.bus.type", "cewm.ce.report.erasure.device.serial.number", "cewm.ce.report.erasure.device.size", "cewm.ce.report.erasure.device.sectors", "cewm.ce.report.erasure.device.sector.size", "cewm.ce.report.erasure.device.remapped.sectors", "cewm.ce.report.erasure.device.hpa", "cewm.ce.report.erasure.device.dco", "cewm.ce.report.erasure.pattern", "cewm.ce.report.erasure.verification.percentage", "cewm.ce.report.erasure.time.start", "cewm.ce.report.erasure.time.end", "cewm.ce.report.erasure.duration", "cewm.ce.report.erasure.status", "cewm.ce.report.erasure.hidden.areas", "cewm.ce.report.erasure.sectors", "cewm.ce.report.erasure.failed.sectors", "cewm.ce.report.erasure.remapped.sectors", "cewm.ce.report.erasure.software.version", "cewm.ce.report.erasure.compliance.requested", "cewm.ce.report.erasure.compliance.resulted", "cewm.ce.report.erasure.smart.health", "cewm.ce.report.erasure.smart.performance", "cewm.ce.report.erasure.smart.erl", "cewm.ce.report.erasure.smart.power.on.time", "cewm.ce.report.erasure.smart.read.errors", "cewm.ce.report.erasure.smart.read.errors.corrected", "cewm.ce.report.erasure.smart.read.errors.uncorrected", "cewm.ce.report.erasure.smart.write.errors", "cewm.ce.report.erasure.smart.write.errors.corrected", "cewm.ce.report.erasure.smart.write.errors.uncorrected", "cewm.ce.report.erasure.smart.verify.errors", "cewm.ce.report.erasure.smart.verify.errors.corrected", "cewm.ce.report.erasure.smart.verify.errors.uncorrected", "cewm.ce.report.status"]
                }
          }`,
        };
    }

    function certusOptionsEq_1(asset_id) {
        // let cond = {}
        // const date = moment();
        let date = moment().format("YYYY-MM-DD");
        let conditions = `{
            "type": "date",
            "operator": "eq",
            "date": ${date}
          }`
        let column = "cewm.ce.report.erasure.time.end";
        if (asset_id) {
            column = "cewm.ce.report.document.custom.field5"
            conditions = `{
            "type": "text",
            "operator": "eq",
            "value": "${asset_id}"
          }`
        }
        return {
            method: "POST",
            url: "https://cloud.certus.software/webservices/rest-api/v1/reports/ce",
            headers: {
                "Customer-Code": "220811",
                Authorization: "Basic dG9yZC5oZW5yeXNvbjpJdG9ub215OEA=",
                "Content-Type": "application/json",
            },
            body: `{
              "reportMode": "ORIGINAL",
              "groupData": "DRIVE",
              "request": {
                "filter": {
                  "criteria": [
                  {
                    "column": ${column},
                    "conditions": [${conditions}]
                    }
                  ],
                  "conjunction": "AND"
                },
                "sort": [{
                  "column": "cewm.ce.report.erasure.time.end",
                  "mode": "DESC"
                }],
                "limit": 1000,
                "offset": 0
              },
            "response": {
                    "showColumns": ["cewm.ce.report.hardware.system.version","cewm.ce.report.hardware.system.family","cewm.ce.report.erasure.status.warning","cewm.ce.report.document.id", "cewm.ce.report.date", "cewm.ce.report.document.software.version", "cewm.ce.report.document.custom.field1", "cewm.ce.report.document.custom.field2", "cewm.ce.report.document.custom.field3", "cewm.ce.report.document.custom.field4", "cewm.ce.report.document.custom.field5", "cewm.ce.report.document.operator.name", "cewm.ce.report.document.operator.group.name", "cewm.ce.report.hardware.lot.name", "cewm.ce.report.hardware.system.manufacturer", "cewm.ce.report.hardware.system.serial.number", "cewm.ce.report.hardware.system.chassis.type", "cewm.ce.report.hardware.system.model", "cewm.ce.report.hardware.system.uuid", "cewm.ce.report.hardware.system.motherboard", "cewm.ce.report.hardware.system.bios", "cewm.ce.report.hardware.system.processor", "cewm.ce.report.hardware.system.device", "cewm.ce.report.hardware.system.memory", "cewm.ce.report.hardware.system.graphic.card", "cewm.ce.report.hardware.system.sound.card", "cewm.ce.report.hardware.system.network.adapter", "cewm.ce.report.hardware.system.optical.drive", "cewm.ce.report.hardware.system.storage.controller", "cewm.ce.report.hardware.system.peripheral.ports", "cewm.ce.report.hardware.system.battery", "cewm.ce.report.erasure.lot.name", "cewm.ce.report.erasure.device.vendor", "cewm.ce.report.erasure.device.model", "cewm.ce.report.erasure.device.type", "cewm.ce.report.erasure.device.bus.type", "cewm.ce.report.erasure.device.serial.number", "cewm.ce.report.erasure.device.size", "cewm.ce.report.erasure.device.sectors", "cewm.ce.report.erasure.device.sector.size", "cewm.ce.report.erasure.device.remapped.sectors", "cewm.ce.report.erasure.device.hpa", "cewm.ce.report.erasure.device.dco", "cewm.ce.report.erasure.pattern", "cewm.ce.report.erasure.verification.percentage", "cewm.ce.report.erasure.time.start", "cewm.ce.report.erasure.time.end", "cewm.ce.report.erasure.duration", "cewm.ce.report.erasure.status", "cewm.ce.report.erasure.hidden.areas", "cewm.ce.report.erasure.sectors", "cewm.ce.report.erasure.failed.sectors", "cewm.ce.report.erasure.remapped.sectors", "cewm.ce.report.erasure.software.version", "cewm.ce.report.erasure.compliance.requested", "cewm.ce.report.erasure.compliance.resulted", "cewm.ce.report.erasure.smart.health", "cewm.ce.report.erasure.smart.performance", "cewm.ce.report.erasure.smart.erl", "cewm.ce.report.erasure.smart.power.on.time", "cewm.ce.report.erasure.smart.read.errors", "cewm.ce.report.erasure.smart.read.errors.corrected", "cewm.ce.report.erasure.smart.read.errors.uncorrected", "cewm.ce.report.erasure.smart.write.errors", "cewm.ce.report.erasure.smart.write.errors.corrected", "cewm.ce.report.erasure.smart.write.errors.uncorrected", "cewm.ce.report.erasure.smart.verify.errors", "cewm.ce.report.erasure.smart.verify.errors.corrected", "cewm.ce.report.erasure.smart.verify.errors.uncorrected", "cewm.ce.report.status"]
                  }
            }`,
        };
    }

    function certusOptionsMobileEq_1(asset_id) {
        let conditions = `{
            "type": "date",
            "operator": "eq",
            "date": ${date}
          }`
        let column = "cewm.cemd.report.erasure.end.time";
        if (asset_id) {
            column = "cewm.cemd.report.document.custom.field2"
            conditions = `{
            "type": "text",
            "operator": "eq",
            "value": "${asset_id}"
          }`
        }

        return {
            method: "POST",
            url: "https://cloud.certus.software/webservices/rest-api/v1/reports/cemd",
            headers: {
                "Customer-Code": "220811",
                Authorization: "Basic dG9yZC5oZW5yeXNvbjpJdG9ub215OEA=",
                "Content-Type": "application/json",
            },
            body: `{
                "reportMode": "ORIGINAL",
                    "request": {
                    "filter": {
                        "criteria": [
                            {
                              "column": ${column},
                              "conditions": [${conditions}]
                              }
                            ],
                            "conjunction": "AND"
                    },
                    "sort": [
                        {
                            "column": "cewm.cemd.report.erasure.end.time",
                            "mode": "ASC"
                        }
                    ],
                        "limit": 1000,
                        "offset": 0
                },
                "response": {
                    "showColumns": [
                        "cewm.cemd.report.device.supervised",
                        "cewm.cemd.report.device.find.my",
                        "cewm.cemd.report.document.software.version",
                        "cewm.cemd.report.hardware.system.chassis.type",
                        "cewm.cemd.report.erasure.duration",
                        "cewm.cemd.report.document.id",
                        "cewm.cemd.report.document.custom.field5",
                        "cewm.cemd.report.document.operator.name",
                        "cewm.cemd.report.erasure.status",
                        "cewm.cemd.report.document.custom.field3",
                        "cewm.cemd.report.document.custom.field4",
                        "cewm.cemd.report.document.operator.group.name",
                        "cewm.cemd.report.document.custom.field1",
                        "cewm.cemd.report.document.custom.field2",
                        "cewm.cemd.report.erasure.pattern",
                        "cewm.cemd.report.document.id",
                        "cewm.cemd.report.document.software.version",
                        "cewm.cemd.report.document.operator.name",
                        "cewm.cemd.report.document.lot.name",
                        "cewm.cemd.report.device.manufacturer",
                        "cewm.cemd.report.device.model",
                        "cewm.cemd.report.device.model.number",
                        "cewm.cemd.report.device.serial.number",
                        "cewm.cemd.report.device.imei",
                        "cewm.cemd.report.device.os",
                        "cewm.cemd.report.device.os.version",
                        "cewm.cemd.report.device.ram",
                        "cewm.cemd.report.device.ecid",
                        "cewm.cemd.report.device.meid",
                        "cewm.cemd.report.device.udid",
                        "cewm.cemd.report.erasure.start.time",
                        "cewm.cemd.report.erasure.end.time",
                        "cewm.cemd.report.erasure.status",
                        "cewm.cemd.report.erasure.compliance",
                        "cewm.cemd.report.device.battery.health",
                        "cewm.cemd.report.device.memory.internal"
    
                    ]
                }
    
            }`,
        };
    }

    function certusOptionsEq(asset_id) {
        // let cond = {}
        // const date = moment();
        let date = moment().format("YYYY-MM-DD");
        return {
            method: "POST",
            url: "https://cloud.certus.software/webservices/rest-api/v1/reports/ce",
            headers: {
                "Customer-Code": "220811",
                Authorization: "Basic dG9yZC5oZW5yeXNvbjpJdG9ub215OEA=",
                "Content-Type": "application/json",
            },
            body: `{
              "reportMode": "ORIGINAL",
              "groupData": "DRIVE",
              "request": {
                "filter": {
                  "criteria": [
                  {
                    "column": "cewm.ce.report.erasure.time.end",
                            "conditions": [{
                            "type": "date",
                            "operator": "eq",
                            "date": ${date}
                            }]
                        }
                  ],
                  "conjunction": "AND"
                },
                "sort": [{
                  "column": "cewm.ce.report.erasure.time.end",
                  "mode": "DESC"
                }],
                "limit": 1000,
                "offset": 0
              },
            "response": {
                    "showColumns": ["cewm.ce.report.hardware.system.version","cewm.ce.report.hardware.system.family","cewm.ce.report.erasure.status.warning","cewm.ce.report.document.id", "cewm.ce.report.date", "cewm.ce.report.document.software.version", "cewm.ce.report.document.custom.field1", "cewm.ce.report.document.custom.field2", "cewm.ce.report.document.custom.field3", "cewm.ce.report.document.custom.field4", "cewm.ce.report.document.custom.field5", "cewm.ce.report.document.operator.name", "cewm.ce.report.document.operator.group.name", "cewm.ce.report.hardware.lot.name", "cewm.ce.report.hardware.system.manufacturer", "cewm.ce.report.hardware.system.serial.number", "cewm.ce.report.hardware.system.chassis.type", "cewm.ce.report.hardware.system.model", "cewm.ce.report.hardware.system.uuid", "cewm.ce.report.hardware.system.motherboard", "cewm.ce.report.hardware.system.bios", "cewm.ce.report.hardware.system.processor", "cewm.ce.report.hardware.system.device", "cewm.ce.report.hardware.system.memory", "cewm.ce.report.hardware.system.graphic.card", "cewm.ce.report.hardware.system.sound.card", "cewm.ce.report.hardware.system.network.adapter", "cewm.ce.report.hardware.system.optical.drive", "cewm.ce.report.hardware.system.storage.controller", "cewm.ce.report.hardware.system.peripheral.ports", "cewm.ce.report.hardware.system.battery", "cewm.ce.report.erasure.lot.name", "cewm.ce.report.erasure.device.vendor", "cewm.ce.report.erasure.device.model", "cewm.ce.report.erasure.device.type", "cewm.ce.report.erasure.device.bus.type", "cewm.ce.report.erasure.device.serial.number", "cewm.ce.report.erasure.device.size", "cewm.ce.report.erasure.device.sectors", "cewm.ce.report.erasure.device.sector.size", "cewm.ce.report.erasure.device.remapped.sectors", "cewm.ce.report.erasure.device.hpa", "cewm.ce.report.erasure.device.dco", "cewm.ce.report.erasure.pattern", "cewm.ce.report.erasure.verification.percentage", "cewm.ce.report.erasure.time.start", "cewm.ce.report.erasure.time.end", "cewm.ce.report.erasure.duration", "cewm.ce.report.erasure.status", "cewm.ce.report.erasure.hidden.areas", "cewm.ce.report.erasure.sectors", "cewm.ce.report.erasure.failed.sectors", "cewm.ce.report.erasure.remapped.sectors", "cewm.ce.report.erasure.software.version", "cewm.ce.report.erasure.compliance.requested", "cewm.ce.report.erasure.compliance.resulted", "cewm.ce.report.erasure.smart.health", "cewm.ce.report.erasure.smart.performance", "cewm.ce.report.erasure.smart.erl", "cewm.ce.report.erasure.smart.power.on.time", "cewm.ce.report.erasure.smart.read.errors", "cewm.ce.report.erasure.smart.read.errors.corrected", "cewm.ce.report.erasure.smart.read.errors.uncorrected", "cewm.ce.report.erasure.smart.write.errors", "cewm.ce.report.erasure.smart.write.errors.corrected", "cewm.ce.report.erasure.smart.write.errors.uncorrected", "cewm.ce.report.erasure.smart.verify.errors", "cewm.ce.report.erasure.smart.verify.errors.corrected", "cewm.ce.report.erasure.smart.verify.errors.uncorrected", "cewm.ce.report.status"]
                  }
            }`,
        };
    }


    function certusOptionsMobileRange(dateTo, dateFrom) {

        return {
            method: "POST",
            url: "https://cloud.certus.software/webservices/rest-api/v1/reports/cemd",
            headers: {
                "Customer-Code": "220811",
                Authorization: "Basic dG9yZC5oZW5yeXNvbjpJdG9ub215OEA=",
                "Content-Type": "application/json",
            },
            body: `{
                "reportMode": "ORIGINAL",
                    "request": {
                    "filter": {
                        "criteria": [
                            {
                                "column": "cewm.cemd.report.erasure.end.time",
                                "conditions": [
                                    {
                                        "type": "date",
                                        "operator": "inRange",
                                        "date": ${dateFrom},
                                        "dateTo": ${dateTo}
                                    }
                                ]
                            }
                        ],
                            "conjunction": "AND"
                    },
                    "sort": [
                        {
                            "column": "cewm.cemd.report.erasure.end.time",
                            "mode": "ASC"
                        }
                    ],
                        "limit": 1000,
                        "offset": 0
                },
                "response": {
                    "showColumns": [
                        "cewm.cemd.report.device.supervised",
                        "cewm.cemd.report.device.find.my",
                        "cewm.cemd.report.document.software.version",
                        "cewm.cemd.report.hardware.system.chassis.type",
                        "cewm.cemd.report.erasure.duration",
                        "cewm.cemd.report.document.id",
                        "cewm.cemd.report.document.custom.field5",
                        "cewm.cemd.report.document.operator.name",
                        "cewm.cemd.report.erasure.status",
                        "cewm.cemd.report.document.custom.field3",
                        "cewm.cemd.report.document.custom.field4",
                        "cewm.cemd.report.document.operator.group.name",
                        "cewm.cemd.report.document.custom.field1",
                        "cewm.cemd.report.document.custom.field2",
                        "cewm.cemd.report.erasure.pattern",
                        "cewm.cemd.report.document.id",
                        "cewm.cemd.report.document.software.version",
                        "cewm.cemd.report.document.operator.name",
                        "cewm.cemd.report.document.lot.name",
                        "cewm.cemd.report.device.manufacturer",
                        "cewm.cemd.report.device.model",
                        "cewm.cemd.report.device.model.number",
                        "cewm.cemd.report.device.serial.number",
                        "cewm.cemd.report.device.imei",
                        "cewm.cemd.report.device.os",
                        "cewm.cemd.report.device.os.version",
                        "cewm.cemd.report.device.ram",
                        "cewm.cemd.report.device.ecid",
                        "cewm.cemd.report.device.meid",
                        "cewm.cemd.report.device.udid",
                        "cewm.cemd.report.erasure.start.time",
                        "cewm.cemd.report.erasure.end.time",
                        "cewm.cemd.report.erasure.status",
                        "cewm.cemd.report.erasure.compliance",
                        "cewm.cemd.report.device.battery.health",
                        "cewm.cemd.report.device.memory.internal"
    
                    ]
                }
    
            }`,
        };
    }

    function certusOptionsMobileEq(asset_id) {
        // let cond = {}
        // const date = moment();
        let date = moment().format("YYYY-MM-DD");
        // console.log("date", date)
        // "column": "cewm.cemd.report.erasure.end.time",
        // "conditions": [{
        // "type": "date",
        // "operator": "eq",
        // "date": ${date}

        // "column": "cewm.cemd.report.document.custom.field2",
        // "conditions": [
        //   {
        //     "type": "text",
        //     "operator": "eq",
        //     "value": "${asset_id}"
        //   }
        // ]
        //     let conditions = `{
        //         "type": "date",
        //         "operator": "eq",
        //         "date": ${date}
        //       }`
        //   if(asset_id){
        //     conditions = `{
        //         "type": "text",
        //         "operator": "eq",
        //         "value": "${asset_id}"
        //       }`
        //   }
        return {
            method: "POST",
            url: "https://cloud.certus.software/webservices/rest-api/v1/reports/cemd",
            headers: {
                "Customer-Code": "220811",
                Authorization: "Basic dG9yZC5oZW5yeXNvbjpJdG9ub215OEA=",
                "Content-Type": "application/json",
            },
            body: `{
                "reportMode": "ORIGINAL",
                    "request": {
                    "filter": {
                        "criteria": [
                            {
                                "column": "cewm.cemd.report.erasure.end.time",
                                "conditions": [{
                                    "type": "date",
                                    "operator": "eq",
                                    "date": ${date}
                                  }
                                ]
                              }
                        ],
                            "conjunction": "AND"
                    },
                    "sort": [
                        {
                            "column": "cewm.cemd.report.erasure.end.time",
                            "mode": "ASC"
                        }
                    ],
                        "limit": 1000,
                        "offset": 0
                },
                "response": {
                    "showColumns": [
                        "cewm.cemd.report.device.supervised",
                        "cewm.cemd.report.device.find.my",
                        "cewm.cemd.report.document.software.version",
                        "cewm.cemd.report.hardware.system.chassis.type",
                        "cewm.cemd.report.erasure.duration",
                        "cewm.cemd.report.document.id",
                        "cewm.cemd.report.document.custom.field5",
                        "cewm.cemd.report.document.operator.name",
                        "cewm.cemd.report.erasure.status",
                        "cewm.cemd.report.document.custom.field3",
                        "cewm.cemd.report.document.custom.field4",
                        "cewm.cemd.report.document.operator.group.name",
                        "cewm.cemd.report.document.custom.field1",
                        "cewm.cemd.report.document.custom.field2",
                        "cewm.cemd.report.erasure.pattern",
                        "cewm.cemd.report.document.id",
                        "cewm.cemd.report.document.software.version",
                        "cewm.cemd.report.document.operator.name",
                        "cewm.cemd.report.document.lot.name",
                        "cewm.cemd.report.device.manufacturer",
                        "cewm.cemd.report.device.model",
                        "cewm.cemd.report.device.model.number",
                        "cewm.cemd.report.device.serial.number",
                        "cewm.cemd.report.device.imei",
                        "cewm.cemd.report.device.os",
                        "cewm.cemd.report.device.os.version",
                        "cewm.cemd.report.device.ram",
                        "cewm.cemd.report.device.ecid",
                        "cewm.cemd.report.device.meid",
                        "cewm.cemd.report.device.udid",
                        "cewm.cemd.report.erasure.start.time",
                        "cewm.cemd.report.erasure.end.time",
                        "cewm.cemd.report.erasure.status",
                        "cewm.cemd.report.erasure.compliance",
                        "cewm.cemd.report.device.battery.health",
                        "cewm.cemd.report.device.memory.internal"
    
                    ]
                }
    
            }`,
        };
    }

    async function insertCertusData(data, limit) {
        let certusData = [];
        let updatecertusData = [];

        let assetsData = _.sortBy(JSON.parse(data), function (o) {
            return new Date(o["cewm.ce.report.erasure.time.end"]).getTime() * 1;
        })
        const certusLists = await certusService.readByQuery({
            fields: ["id", "device_serial_number", "document_id", "erasure_status", "asset_id", "erasure_status", "device_serial_number", "id", "erasure_time_end", "device_size"],
            limit: 2000,
            sort: ['-created_date']
        })
        // console.log("certusLists", certusLists.length)
        if (certusLists?.length > 0) {
            const assetLists = await assetsService.readByQuery({
                fields: ["asset_id", "asset_id", "status", "grade", "platform", "project_id"],
                limit: limit,
                sort: ['-date_created'],

            });
            // console.log("assetLists", assetLists.length)

            if (assetLists?.length > 0) {
                try {
                    for (let i = 0; i <= assetsData.length; i++) {
                        if (assetsData[i]) {
                            // console.log("assetsData[i]).asset_id", mapCertusData(assetsData[i]).asset_id)
                            let device_serial_number = assetsData[i]["cewm.ce.report.erasure.device.serial.number"] || ""
                            let doc_id = assetsData[i]["cewm.ce.report.document.id"] || "";
                            let erasure_status = assetsData[i]["cewm.ce.report.erasure.status"] || "";
                            let certusFilter = certusLists.filter((obj) => (obj?.device_serial_number === device_serial_number) && (obj?.document_id === doc_id) && obj.erasure_status === erasure_status);
                            if (certusFilter && certusFilter?.length === 0) {
                                const activity = await certusService.createOne(
                                    mapCertusData(assetsData[i])
                                ).then(async (response1) => {
                                    if (!mapCertusData(assetsData[i]).asset_id) {
                                        return
                                    }
                                    await insertAssetsSingle(assetsData[i], assetLists, certusLists)

                                }).catch((error1) => {
                                    // res = []
                                    console.log("Certus service error==>", error1)
                                });
                            } else {
                                updatecertusData.push(mapCertusData(assetsData[i]));
                                updatecertusData = _.sortBy(certusData, function (o) { return new Date(o.erasure_ended).getTime() * -1; })
                                let projectID = mapCertusData(assetsData[i]).project_id;
                                let sql = `update public."Certus" set project_id = '${projectID}', 
                                                    asset_id = '${mapCertusData(assetsData[i]).asset_id}', keyboard = '${mapCertusData(assetsData[i]).keyboard}', complaint = '${mapCertusData(assetsData[i]).complaint}',
                                                    grade = '${mapCertusData(assetsData[i]).grade}' where
                                                    device_serial_number = '${device_serial_number}' and document_id = '${doc_id}'`
                                database.raw(sql)
                                    .then(async (results) => {
                                        if (!mapCertusData(assetsData[i]).asset_id) {
                                            return
                                        }
                                        await insertAssetsSingle(assetsData[i], assetLists, certusLists)


                                    })
                                    .catch((error) => {
                                        console.log("update certus error ==>", error)
                                    });
                            }
                        }
                    }
                } catch (err) {
                    console.log("not fetch error ==>", err)
                    throw err
                }
            }
        }



        // const insertCertusData = await queries.insertCertusMultipleCron("Certus", certusData);
    }

    async function updateHddValues(certusData, assetsData, assetLists, certusLists) {
        try {
            const certus = certusLists.filter((obj) => obj.asset_id.toString() === assetsData.asset_id.toString())


            // certusService.readByQuery({
            //     fields: ["asset_id", "erasure_status", "device_serial_number", "id", "erasure_time_end", "device_size"],
            //     filter: {
            //         asset_id: {
            //             _nempty: true,
            //         },
            //         asset_id: {
            //             _contains: `${assetsData.asset_id}`,
            //         }
            //     },
            // });
            if (certus && certus?.length > 0) {
                let uniqueValue = certus.sort((a, b) => b.id - a.id);
                uniqueValue = _.uniq(uniqueValue, 'device_serial_number');
                let hdd_serial_number = []
                let data_destruction = []
                let device_size = []
                let hdd_count = uniqueValue.length

                _.map(uniqueValue, function (group) {
                    hdd_serial_number.push(group.device_serial_number);
                    data_destruction.push(group.erasure_status);
                    device_size.push(group.device_size);
                });

                let hdd_serial_number1 = hdd_serial_number.join(' / ')
                let data_destruction1 = data_destruction.join(' / ')
                let device_size1 = device_size.join(' / ');
                let certus_Data = getCertusMapped(certusData, assetsData.asset_id)

                delete certus_Data.date_nor;
                delete certus_Data.sold_price;
                delete certus_Data.sold_order_nr;
                delete certus_Data.sample_co2;
                delete certus_Data.sample_weight;
                delete certus_Data.target_price;
                delete certus_Data.hdd_count;
                delete certus_Data.Part_No
                certus_Data.hdd_count = `${hdd_count}`;
                certus_Data.hdd = `${device_size1}`;
                certus_Data.data_destruction = `${data_destruction1}`;
                certus_Data.wipe_standard = `${assetsData.easure_pattern}`;
                certus_Data.erasure_ended = `${assetsData.erasure_ended}`;
                certus_Data.hdd_serial_number = `${hdd_serial_number1}`;
                certus_Data.grade = `${assetsData.grade}`;
                certus_Data.complaint = `${assetsData.complaint}`;
                // const assetResult = await assetsService.readByQuery({
                //     fields: ["asset_id", "status", "grade", "platform"],
                //     filter: {
                //         asset_id: {
                //             _eq: assetsData.asset_id,
                //         },
                //         status: {
                //             _nin: ['SOLD', 'RESERVATION'],
                //         }
                //     },
                // });
                const assetResult = assetLists.filter((obj) => obj.asset_id === assetsData.asset_id)
                if (assetResult?.length > 0 && assetResult[0].status !== 'RESERVATION' && assetResult[0].status !== 'SOLD') {
                    if (assetResult[0].status === 'IN STOCK' && certus_Data.status === 'NOT ERASED' && assetResult[0].platform === 'MOBILE_UPDATE') {
                        certus_Data.platform = 'MOBILE_UPDATE_CERTUS';
                    } else {
                        delete certus_Data.status;
                    }
                    if (assetResult[0].grade && !certus_Data.grade) {
                        certus_Data.grade = assetResult[0].grade
                    }
                    if (!certus_Data.project_id) {
                        delete certus_Data.project_id;
                    }
                    return await assetsService.updateOne(assetsData.asset_id,
                        certus_Data
                    ).then(async (response1) => {
                        // res.json(response);
                        // console.log("asset updated==>", response1)
                        if (assetResult.status === 'NOT ERASED' && certus_Data.data_destruction && certus_Data.data_destruction.toUpperCase() === 'ERASED') {
                            return await assetsService.updateOne(certus_Data.asset_id,
                                { status: 'IN STOCK' }
                            ).then((response1) => {
                                // res.json(response);
                                // console.log("asset update success computer", certus_Data.asset_id)
                            }).catch((error1) => {
                                console.log("asset update certus error==>", error1)
                            });
                        }
                    }).catch((error1) => {
                        console.log(certus_Data.asset_id, "asset update error 3", error1)
                    });
                } else {
                    console.log("asset not update==>", assetResult[0]?.status)
                }
            }
        } catch (error) {
            throw new ServiceUnavailableException(error);
        }

    }

    function mapCertusData(field) {
        let erasure_end = field["cewm.ce.report.erasure.time.end"]?.split(" ");
        let erasure_enddate = ''
        if (erasure_end) {
            erasure_end.splice(3 - 1, 1);
            erasure_enddate = erasure_end.join(" ");
        }

        const certusdata = {
            erasure_ended: erasure_enddate || '',
            erasure_with_warning: field["cewm.ce.report.erasure.status.warning"] || '',
            project_id: field["cewm.ce.report.document.custom.field1"] || '',
            grade: field["cewm.ce.report.document.custom.field4"] || '',
            asset_id: field["cewm.ce.report.document.custom.field5"] || '',
            document_id: field["cewm.ce.report.document.id"] || '',
            report_date: field["cewm.ce.report.date"] || '',
            software_version: field["cewm.ce.report.document.software.version"] || '',
            report_status: field["cewm.ce.report.status"] || '',
            lot_number: field["cewm.ce.report.hardware.lot.name"] || '',
            system_manufacturer: field["cewm.ce.report.hardware.system.manufacturer"] || '',
            serial_number: field["cewm.ce.report.hardware.system.serial.number"] || '',
            chasis_type: field["cewm.ce.report.hardware.system.chassis.type"] || '',
            system_model: field["cewm.ce.report.hardware.system.model"] || '',
            uuid: field["cewm.ce.report.hardware.system.uuid"] || '',
            motherboard: field["cewm.ce.report.hardware.system.motherboard"] || '',
            bios: field["cewm.ce.report.hardware.system.bios"] || '',
            processor: field["cewm.ce.report.hardware.system.processor"] || '',
            device: field["cewm.ce.report.hardware.system.device"] || '',
            memory: field["cewm.ce.report.hardware.system.memory"] || '',
            graphic_card: field["cewm.ce.report.hardware.system.graphic.card"] || '',
            sound_card: field["cewm.ce.report.hardware.system.sound.card"] || '',
            adapter: field["cewm.ce.report.hardware.system.network.adapter"] || '',
            optical_drive: field["cewm.ce.report.hardware.system.optical.drive"] || '',
            controller: field["cewm.ce.report.hardware.system.storage.controller"] || '',
            peripheral_ports: field["cewm.ce.report.hardware.system.peripheral.ports"] || '',
            battery: field["cewm.ce.report.hardware.system.battery"] || '',
            device_hpa: field["cewm.ce.report.erasure.device.hpa"] || '',
            device_dco: field["cewm.ce.report.erasure.device.dco"] || '',
            easure_pattern: field["cewm.ce.report.erasure.pattern"] || '',
            verification_percentage: field["cewm.ce.report.erasure.verification.percentage"] || '',
            erasure_time_start: field["cewm.ce.report.erasure.time.start"] || '',
            erasure_time_end: field["cewm.ce.report.erasure.time.end"] || '',
            erasure_duration: field["cewm.ce.report.erasure.duration"] || '',
            erasure_status: field["cewm.ce.report.erasure.status"] || '',
            erasure_hidden_area: field["cewm.ce.report.erasure.hidden.areas"] || '',
            erasure_sectors: field["cewm.ce.report.erasure.sectors"] || '',
            erasure_failed_sector: field["cewm.ce.report.erasure.failed.sectors"] || '',
            erasure_remapped_sectors: field["cewm.ce.report.erasure.remapped.sectors"] || '',
            software_version: field["cewm.ce.report.erasure.software.version"] || '',
            compliane_requested: field["cewm.ce.report.erasure.compliance.requested"] || '',
            compliane_resulted: field["cewm.ce.report.erasure.compliance.resulted"] || '',
            smart_health: field["cewm.ce.report.erasure.smart.health"] || '',
            performance: field["cewm.ce.report.erasure.smart.performance"] || '',
            keyboard: field["cewm.ce.report.document.custom.field2"] || '',
            complaint: field["cewm.ce.report.document.custom.field3"] || '',
            operator_name: field["cewm.ce.report.document.operator.name"] || '',
            operator_group_name: field["cewm.ce.report.document.operator.group.name"] || '',
            smart_erl: field["cewm.ce.report.erasure.smart.erl"] || '',
            power_on_time: field["cewm.ce.report.erasure.smart.power.on.time"] || '',
            read_errors: field["cewm.ce.report.erasure.smart.read.errors"] || '',
            errors_corrected: field["cewm.ce.report.erasure.smart.read.errors.corrected"] || '',
            errors_uncorrected: field["cewm.ce.report.erasure.smart.read.errors.uncorrected"] || '',
            write_errors: field["cewm.ce.report.erasure.smart.write.errors"] || '',
            write_errors_correced: field["cewm.ce.report.erasure.smart.write.errors.corrected"] || '',
            write_errors_uncorreced: field["cewm.ce.report.erasure.smart.write.errors.uncorrected"] || '',
            verify_errors: field["cewm.ce.report.erasure.smart.verify.errors"] || '',
            verify_errors_correced: field["cewm.ce.report.erasure.smart.verify.errors.corrected"] || '',
            verify_errors_uncorreced: field["cewm.ce.report.erasure.smart.verify.errors.uncorrected"] || '',
            erasure_lot_name: field["cewm.ce.report.erasure.lot.name"] || '',
            device_vendor: field["cewm.ce.report.erasure.device.vendor"] || '',
            device_model: field["cewm.ce.report.erasure.device.model"] || '',
            device_type: field["cewm.ce.report.erasure.device.type"] || '',
            bus_type: field["cewm.ce.report.erasure.device.bus.type"] || '',
            device_serial_number: field["cewm.ce.report.erasure.device.serial.number"] || '',
            device_size: field["cewm.ce.report.erasure.device.size"] || '',
            device_sectors: field["cewm.ce.report.erasure.device.sectors"] || '',
            device_sector_size: field["cewm.ce.report.erasure.device.sector.size"] || '',
            remapped_sectors: field["cewm.ce.report.erasure.device.remapped.sectors"] || '',
            model_lenova: field["cewm.ce.report.hardware.system.family"]
        }
        return certusdata
    }

    async function insertAssetsSingle(field, assetLists, certusLists) {
        if (!field["cewm.ce.report.erasure.time.end"]) {
            return
        }
        let asset_Id = field["cewm.ce.report.document.custom.field5"]
            ? field["cewm.ce.report.document.custom.field5"]
                .replace(/[\W_]/g, "")
                .replace(/\D/g, "")
            : "";
        let project_Id = field["cewm.ce.report.document.custom.field1"]
            ? field["cewm.ce.report.document.custom.field1"]
            : null;
        let lotnumber = field["cewm.ce.report.erasure.lot.name"];
        let groupname = field["cewm.ce.report.document.operator.group.name"];
        if (groupname && groupname.toUpperCase() === 'TPL') { // allow only TPL user
            if (lotnumber !== 2000 && asset_Id) {
                if (!lotnumber || lotnumber !== 2000) {
                    const alreadexists = assetLists.filter((obj) => obj.asset_id === asset_Id)
                    // console.log("alreadexists", alreadexists.length)
                    if (alreadexists.length === 0) {
                        let certus_Data = getCertusMapped(field, asset_Id);
                        if (certus_Data.complaint) {
                            let complaint = certus_Data.complaint.toLowerCase()
                            if (complaint.includes("hdd from") || complaint.includes("hdds from")) {
                                certus_Data.asset_type = 'HDD';
                                certus_Data.form_factor = 'HDD';
                                certus_Data.manufacturer = "";
                                certus_Data.model = "";
                                certus_Data.processor = "";
                                certus_Data.memory = "";
                                certus_Data.imei = "";
                                certus_Data.graphic_card = "";
                                certus_Data.serial_number = "";
                                certus_Data.optical = "";
                                certus_Data.battery = "";
                                certus_Data.keyboard = "";
                            }
                        }
                        return await assetsService.createOne(
                            certus_Data
                        ).then((response1) => {
                            // res.json(response);
                            console.log("asset create success single", response1)
                        }).catch((error1) => {
                            console.log("asset create error ==>", asset_Id)
                        });
                    } else {
                        if (asset_Id) {
                            //update HDD valuessss
                            await updateHddValues(field, mapCertusData(field), assetLists, certusLists);
                        }
                    }
                }
            }
        }
    }


    // async function insertAssets(data) {

    //     let sortcertusData = _.sortBy(JSON.parse(data), function (o) {
    //         return new Date(o["cewm.ce.report.erasure.time.end"]).getTime() * 1;
    //     })

    //     sortcertusData.map(async (field) => {
    //         if (!field["cewm.ce.report.erasure.time.end"]) {
    //             return
    //         }
    //         let asset_Id = field["cewm.ce.report.document.custom.field5"]
    //             ? field["cewm.ce.report.document.custom.field5"]
    //                 .replace(/[\W_]/g, "")
    //                 .replace(/\D/g, "")
    //             : "";
    //         let lotnumber = field["cewm.ce.report.erasure.lot.name"];
    //         if ((!lotnumber || lotnumber !== 2000) && asset_Id && field["cewm.ce.report.document.custom.field1"]) {
    //             const alreadexists = await assetsService.readByQuery({
    //                 fields: ["asset_id"],
    //                 filter: {
    //                     asset_id: {
    //                         _eq: asset_Id,
    //                     }
    //                 },
    //             });

    //             let certus_Data = getCertusMapped(field, asset_Id);
    //             if (certus_Data.complaint) {
    //                 let complaint = certus_Data.complaint.toLowerCase()
    //                 if (complaint.includes("hdd from") || complaint.includes("hdds from")) {
    //                     certus_Data.asset_type = 'HDD';
    //                     certus_Data.form_factor = 'HDD';
    //                     certus_Data.manufacturer = "";
    //                     certus_Data.model = "";
    //                     certus_Data.processor = "";
    //                     certus_Data.memory = "";
    //                     certus_Data.imei = "";
    //                     certus_Data.graphic_card = "";
    //                     certus_Data.serial_number = "";
    //                     certus_Data.optical = "";
    //                     certus_Data.battery = "";
    //                     certus_Data.keyboard = "";
    //                 }
    //             }
    //             if (alreadexists.length === 0) {
    //                 if (field["cewm.ce.report.document.operator.group.name"] && field["cewm.ce.report.document.operator.group.name"].toUpperCase() == 'TPL') {
    //                     return await assetsService.createOne(
    //                         certus_Data
    //                     ).then((response1) => {
    //                         // res.json(response);
    //                         console.log("asset createated", response1)
    //                     }).catch((error1) => {
    //                         console.log("asset create errrrr", asset_Id)
    //                     });
    //                 }
    //             } else {
    //                 if (asset_Id) {
    //                     if (alreadexists[0] && alreadexists[0].status === 'NOT ERASED' && certus_Data.data_destruction && certus_Data.data_destruction.toUpperCase() === 'ERASED') {
    //                         return await assetsService.updateOne(certus_Data.asset_id,
    //                             { status: 'IN STOCK' }
    //                         ).then((response1) => {
    //                             // res.json(response);
    //                             console.log("asset update success computer", certus_Data.asset_id)
    //                         }).catch((error1) => {
    //                             console.log("asset update certus  errrrr", error1)
    //                         });
    //                     }
    //                 }
    //             }

    //         }
    //     });
    // }

    function getCertusMapped(field, asset_Id, tempOffset) {
        let processor = field["cewm.ce.report.hardware.system.processor"].split(";");
        let processor1 = ''
        // let processor2 = ''
        // let processor3 = ''
        // let processor4 = ''
        if (processor[2]) {
            processor1 = processor_bk = processor[2].split(":")[1].trim();
        }
        if (processor1.includes("Not Specified")) {
            processor1 = processor[1].split(":")[1].trim();
        }
        removeprocessorData().forEach((removable) => {
            processor1 = processor1.toUpperCase().replace(removable.toUpperCase(), "").trim();
        })
        if (processor1.includes("CPU @")) {
            processor1 = processor1.split(' CPU @ ')[0]
        } else if (processor1.includes(" @ ")) {
            processor1 = processor1.split(' @ ')[0]
        }
        processor = processor1;
        let battery = "";
        let battery1 = "";
        let battery2 = "";
        if (field["cewm.ce.report.hardware.system.battery"]) {
            let battery_split = field["cewm.ce.report.hardware.system.battery"].split(
                ";"
            );
            if (battery_split[4]) {
                battery1 = battery_split[4].split(":")[1].trim();
            }
            if (battery_split[5]) {
                battery2 = battery_split[5].trim();
            }
            battery = battery1 + " " + battery2;
        }
        // console.log(field["cewm.ce.report.document.custom.field5"], "battery", battery)

        let memoryMB = 0;
        let memoryGB = 0;
        let memory = 0;
        if (field["cewm.ce.report.hardware.system.memory"]) {
            let memory_split = field["cewm.ce.report.hardware.system.memory"].split(
                ";"
            );
            for (let i = 0; i <= memory_split.length; i++) {
                if (memory_split[i] && memory_split[i].includes("MB")) {
                    memoryMB += parseInt(memory_split[i].replace("MB", "").trim());
                }
                if (memory_split[i] && memory_split[i].includes("GB")) {
                    memoryGB += parseInt(memory_split[i].replace("GB", "").trim());
                }
            }
            memoryMB = (parseInt(memoryMB) / 1024);
            memory = parseInt(memoryMB) + parseInt(memoryGB)
        }
        // console.log("memory", memory)
        // console.log(field["cewm.ce.report.document.custom.field5"], "memory", memory)
        let erasure_end = field["cewm.ce.report.erasure.time.end"].split(" ");
        erasure_end.splice(3 - 1, 1);
        let erasure_enddate = erasure_end.join(" ");
        let erasure_start = field["cewm.ce.report.erasure.time.start"].split(" ");
        erasure_start.splice(3 - 1, 1);
        erasure_start = erasure_start.join(" ");
        let status = null;
        let grade = null
        if (field["cewm.ce.report.document.custom.field4"]) {
            grade = field["cewm.ce.report.document.custom.field4"].toUpperCase()
        }

        if (grade === 'A PLUS') {
            grade = 'A+'
        } else if (grade === 'A +') {
            grade = 'A+'
        }
        if (grade === 'A' || grade === 'B' || grade === 'C') {
            status = 'IN STOCK';
        } else if (grade === 'D') {
            status = "HARVEST";
        } else if (grade === 'E') {
            status = "RECYCLED";
        } else {
            status = 'IN STOCK';
        }
        let dataDestruction = field["cewm.ce.report.erasure.status"].toLowerCase();
        let complaints = field["cewm.ce.report.document.custom.field3"].toUpperCase();
        if (
            dataDestruction === "erasure in progress"
            || dataDestruction === "not erased/not erased/not erased"
            || dataDestruction === "not erased/not erased"
            || dataDestruction === "failed sectors"
            || dataDestruction.includes("erased with warning(s) (reallocated sectors not erased: ")
            || dataDestruction.includes("erased with warnings (reallocated sectors not erased: ")
            || dataDestruction.includes("not erased (")) {
            status = 'NOT ERASED';
            if (complaints.includes("HDD REM")) {
                status = 'IN STOCK';
            }
        }
        let model = field["cewm.ce.report.hardware.system.model"];
        let form_factor = field["cewm.ce.report.hardware.system.chassis.type"];
        let manufacturer = field["cewm.ce.report.hardware.system.manufacturer"];
        if (manufacturer && manufacturer.toUpperCase() === "HEWLETT-PACKARD") {
            manufacturer = "HP";
        }
        let Part_No = null
        if (manufacturer?.toUpperCase() === 'LENOVO') {
            Part_No = model.split(' ').pop();
            model = field["cewm.ce.report.hardware.system.family"]
        }

        let updateData = {
            asset_id: asset_Id,
            asset_id_1: asset_Id,
            "Part_No": Part_No,
            erasure_with_warning: field["cewm.ce.report.erasure.status.warning"],
            erasure_lot_name: field["cewm.ce.report.erasure.lot.name"],
            operator_name: field["cewm.ce.report.document.operator.name"],
            manufacturer: manufacturer,
            asset_type: "COMPUTER",
            imei: 0,
            ip: 0,
            quantity: 1,
            model: model,
            form_factor: form_factor,
            processor: processor,
            processor_bk: processor_bk,
            memory: `${memory} GB`,
            system_memory: field["cewm.ce.report.hardware.system.memory"] || null,
            hdd: field["cewm.ce.report.erasure.device.size"]?.replace(".0", "GB"),
            optical: field["cewm.ce.report.hardware.system.optical.drive"] || "",
            graphic_card: field["cewm.ce.report.hardware.system.graphic.card"],

            battery: battery,
            keyboard: field["cewm.ce.report.document.custom.field2"],
            // pallet_number: "",
            serial_number: field["cewm.ce.report.hardware.system.serial.number"] || null,
            hdd_serial_number:
                field["cewm.ce.report.erasure.device.serial.number"] || null,
            data_destruction: field["cewm.ce.report.erasure.status"],
            wipe_standard: field["cewm.ce.report.erasure.pattern"],
            erasure_ended: erasure_enddate,
            // previous_erasure_ended: new Date(erasure_enddate),
            grade: field["cewm.ce.report.document.custom.field4"],
            complaint: field["cewm.ce.report.document.custom.field3"],
            project_id: field["cewm.ce.report.document.custom.field1"] || null,
            project_id_1: field["cewm.ce.report.document.custom.field1"] || null,
            status: status,
            sample_co2: 55,
            sample_weight: 2.0,
            erasure_start: erasure_start,
            data_generated: "CERTUS"
        };
        return updateData;
    }

    async function insertMobileCertusData(data, limit) {

        let assetsData = _.sortBy(JSON.parse(data), function (o) {
            return new Date(o["cewm.cemd.report.erasure.end.time"]).getTime() * 1;
        })
        const certusMobileLists = await certusMobileService.readByQuery({
            fields: ["id", "serial_number", "document_id", "erasure_status"],
            limit: limit,
            sort: ['-date_created'],
            // filter: {
            //     asset_type: {
            //         _icontains: `MOBILE`,
            //     }
            // },
        })
        if (certusMobileLists) {
            const assetLists = await assetsService.readByQuery({
                fields: ["asset_id", "status", "grade", "data_destruction", "platform", "grade_from_app", "project_id"],
                limit: limit + limit,
                sort: ['-date_created'],
                // filter: {
                //     data_generated: {
                //         _eq: `CERTUS MOBILE DEVICE`,
                //     }
                // },
            });
            for (let i = 0; i <= assetsData.length; i++) {
                if (assetsData[i]) {
                    // console.log(assetsData[i]["cewm.cemd.report.document.custom.field1"], "assetsData[i]", assetsData[i]["cewm.cemd.report.device.serial.number"])
                    let project_id = '';
                    let asset_id = '';
                    let serial_number = '';
                    let doc_id = '';
                    let status = '';
                    let battery = '';
                    let internal = '';
                    let part_no = null;
                    let grade = null;
                    let comments = null
                    if (assetsData[i]?.["cewm.cemd.report.document.custom.field1"]) {
                        project_id = assetsData[i]["cewm.cemd.report.document.custom.field1"]
                    }
                    if (assetsData[i]?.["cewm.cemd.report.document.custom.field2"]) {
                        asset_id = assetsData[i]["cewm.cemd.report.document.custom.field2"]
                    }
                    if (assetsData[i]?.["cewm.cemd.report.device.serial.number"]) {
                        serial_number = assetsData[i]["cewm.cemd.report.device.serial.number"]
                    }
                    if (assetsData[i]?.["cewm.cemd.report.document.id"]) {
                        doc_id = assetsData[i]["cewm.cemd.report.document.id"]
                    }
                    if (assetsData[i]?.["cewm.cemd.report.erasure.status"]) {
                        status = assetsData[i]["cewm.cemd.report.erasure.status"]
                    }
                    if (assetsData[i]?.["cewm.cemd.report.device.battery.health"]) {
                        battery = assetsData[i]["cewm.cemd.report.device.battery.health"]
                    }
                    if (assetsData[i]?.["cewm.cemd.report.device.memory.internal"]) {
                        internal = assetsData[i]["cewm.cemd.report.device.memory.internal"]
                    }
                    if (assetsData[i]?.["cewm.cemd.report.device.model.number"]) {
                        part_no = assetsData[i]["cewm.cemd.report.device.model.number"]
                    }
                    if (assetsData[i]?.["cewm.cemd.report.document.custom.field3"]) {
                        grade = assetsData[i]["cewm.cemd.report.document.custom.field3"]
                    }
                    if (assetsData[i]?.["cewm.cemd.report.document.custom.field4"]) {
                        comments = assetsData[i]["cewm.cemd.report.document.custom.field4"]
                    }
                    let certusFilter = certusMobileLists.filter((obj) => (obj?.serial_number === serial_number) && (obj?.document_id === doc_id) && obj.erasure_status === status);
                    if (certusFilter && certusFilter?.length === 0) {
                        const activity = await certusMobileService.createOne(
                            getCertusMobileMapped(assetsData[i])
                        ).then(async (response1) => {
                            // console.log(asset_id, "certus asset create success")
                            await insertMobileSingleAssets(await getCertusMobileMapped(assetsData[i]), assetLists)
                            if (!getCertusMobileMapped(assetsData[i]).asset_id) {
                                return
                            }

                        }).catch((error1) => {
                            // res = []mapCertusData
                            console.log(asset_id, "certus asset create error", error1)
                        });
                    } else {
                        let updateData = {
                            device_model_number: part_no,
                            device_battery_health: battery,
                            hdd: internal,
                            asset_id: asset_id,
                            project_id: project_id,
                            grade: grade,
                            comments: comments
                        }
                        await certusMobileService.updateOne(certusFilter[0].id,
                            updateData
                        ).then(async (response1) => {
                            // res.json(response);
                            console.log("update certus mobile update", asset_id)
                            await insertMobileSingleAssets(await getCertusMobileMapped(assetsData[i]), assetLists)
                        }).catch((error1) => {
                            console.log("asset update mobile  error ==>", error1)
                        });

                        // let sqlcertusupdate = `update public."CertusMobileNew" set 
                        //             device_model_number='${part_no}',
                        //             device_battery_health='${battery}',
                        //             hdd='${internal}',
                        //             asset_id = '${asset_id}', 
                        //             project_id='${project_id}', 
                        //             grade = '${grade}',
                        //             comments='${comments}'
                        //             where serial_number = '${serial_number}' and document_id = '${doc_id}'`
                        // database.raw(sqlcertusupdate)
                        //     .then(async (results) => {
                        //         // console.log("resuls, ", serial_number)
                        //         await insertMobileSingleAssets(await getCertusMobileMapped(assetsData[i]))
                        //     })
                        //     .catch((error) => {
                        //         console.log(asset_id, "certus update errrrr", error)
                        //     });
                    }
                }
            }
        }

    }


    async function insertMobileSingleAssets(field, assetLists) {
        if (field.asset_id) {
            const alreadexists = assetLists.filter((obj) => obj.asset_id === field.asset_id)
            let certus_Mobile_Data = await getCertusMobileAssetMapped(field, field.asset_id);
            // console.log("alreadexists mobile certus==>", alreadexists.length)
            if (alreadexists.length === 0) {
                if (field.operator_group_name && field.operator_group_name.toUpperCase() == 'TPL') {
                    assetsService.createOne(
                        certus_Mobile_Data
                    ).then((response1) => {
                        // res.json(response);
                        console.log("asset mobile create success", response1)
                    }).catch((error1) => {
                        console.log("asset mobile create error===>", error1)
                    });
                }
            } else {
                if (field.asset_id && (alreadexists[0]?.status !== 'SOLD' && alreadexists[0]?.status !== 'RESERVATION')) {
                    if (alreadexists[0].grade && !certus_Mobile_Data.grade) {
                        certus_Mobile_Data.grade = alreadexists[0].grade
                    }
                    if (!certus_Mobile_Data.grade && alreadexists[0].grade_from_app) {
                        certus_Mobile_Data.grade = alreadexists[0].grade_from_app
                    }
                    let updateData = {
                        "Part_No": certus_Mobile_Data.Part_No,
                        grade: certus_Mobile_Data.grade,
                        battery: certus_Mobile_Data.battery,
                        complaint: certus_Mobile_Data.complaint,
                        erasure_ended: certus_Mobile_Data.erasure_ended,
                        data_destruction: certus_Mobile_Data.data_destruction,
                        wipe_standard: certus_Mobile_Data.wipe_standard,
                        supervised: certus_Mobile_Data.supervised,
                        find_my_device: certus_Mobile_Data.find_my_device,
                        asset_id: field.asset_id,
                        project_id: field.project_id,
                        project_id_1: field.project_id,
                        serial_number: field.serial_number,
                        imei: field.device_imei,
                        model: field.model,
                        hdd: field.hdd,
                        manufacturer: field.manufacturer,
                        form_factor: certus_Mobile_Data.form_factor,
                        asset_type: "MOBILE DEVICE"
                    }
                    if (!updateData.project_id) {
                        delete updateData.project_id;
                        delete updateData.project_id_1;
                    }
                    // console.log("updatedd data", updateData)
                    return await assetsService.updateOne(field.asset_id,
                        updateData
                    ).then((response1) => {
                        // res.json(response);
                        console.log(certus_Mobile_Data.Part_No, "asset update success mobile", field.asset_id)
                    }).catch((error1) => {
                        console.log("asset update mobile error ==>", error1)
                    });

                }
            }
        }
    }

    async function getCertusMobileAssetMapped(field, asset_Id, tempOffset) {

        let erasure_enddate = ''
        if (field.erasure_ended) {
            let erasure_end = field.erasure_ended.split(" ");
            erasure_end.splice(3 - 1, 1);
            erasure_enddate = erasure_end.join(" ");
        }
        let erasure_start = ''
        if (field.erasure_start) {
            let erasure_start = field.erasure_start.split(" ");
            erasure_start.splice(3 - 1, 1);
            erasure_start = erasure_start.join(" ");
        }
        let status = null;
        let grade = null
        if (field.grade) {
            grade = field.grade.toUpperCase()
        }
        if (grade === 'A PLUS') {
            grade = 'A+'
        } else if (grade === 'A +') {
            grade = 'A+'
        }
        if (grade === 'A' || grade === 'B' || grade === 'C') {
            status = 'IN STOCK';
        } else if (grade === 'D') {
            status = "HARVEST";
        } else if (grade === 'E') {
            status = "RECYCLED";
        } else {
            status = 'IN STOCK';
        }

        let model = field.device_model;
        let manufacturer = field.system_manufacturer;
        let formfactor = ''
        if (model.toUpperCase().includes("IPHONE") || model.toUpperCase().includes("GALAXY")) {
            formfactor = 'phone'
        }
        if (model.toUpperCase().includes("IPAD")) {
            formfactor = 'tablet'
        }
        let updateData = {
            "Part_No": field.device_model_number,
            asset_id: asset_Id,
            asset_id_1: asset_Id,
            form_factor: formfactor,
            erasure_lot_name: field.erasure_lot_name,
            operator_name: field.operator_group_name,
            manufacturer: manufacturer,
            asset_type: "MOBILE DEVICE",
            imei: field.device_imei,
            quantity: 1,
            model: model,
            battery: `${field.device_battery_health}`,
            hdd: field.device_internal_memory.replace(".0", "GB"),
            serial_number: field.serial_number,
            data_destruction: field.erasure_status,
            erasure_ended: erasure_enddate,
            erasure_start: erasure_start,
            grade: grade,
            complaint: field.comments,
            project_id: field.project_id,
            project_id_1: field.project_id,
            status: 'IN STOCK',
            sample_co2: 55,
            sample_weight: '',
            data_generated: "CERTUS MOBILE DEVICE",
            wipe_standard: field.wipe_standard,
            supervised: field.supervised,
            find_my_device: field.find_my_device
        };
        return updateData;
    }

    function getCertusMobileMapped(field) {
        // console.log(field["cewm.cemd.report.document.custom.field5"], "memory", memory)
        let erasure_enddate = null
        if (field["cewm.cemd.report.erasure.end.time"]) {
            let erasure_end = field["cewm.cemd.report.erasure.end.time"].split(" ");
            erasure_end.splice(3 - 1, 1);
            erasure_enddate = erasure_end.join(" ");
        }
        let erasure_start = null
        if (field["cewm.cemd.report.erasure.start.time"]) {
            erasure_start = field["cewm.cemd.report.erasure.start.time"].split(" ");
            erasure_start.splice(3 - 1, 1);
            erasure_start = erasure_start.join(" ");
        }
        // let erasure_start = field["cewm.cemd.report.erasure.start.time"].split(" ");
        // erasure_start.splice(3 - 1, 1);
        // erasure_start = erasure_start.join(" ");

        let grade = null
        if (field["cewm.cemd.report.document.custom.field3"]) {
            grade = field["cewm.cemd.report.document.custom.field3"].toUpperCase()
        }


        let manufacturer = field["cewm.cemd.report.device.manufacturer"];
        if (manufacturer && manufacturer.toUpperCase() === "HEWLETT-PACKARD") {
            manufacturer = "HP";
        }
        let formfactor = ''
        let model = field["cewm.cemd.report.device.model"];

        if (model?.toUpperCase().includes("IPHONE") || model?.toUpperCase().includes("GALAXY")) {
            formfactor = 'phone'
        }
        if (model?.toUpperCase().includes("IPAD")) {
            formfactor = 'tablet'
        }
        let updateData = {
            erasure_ended: erasure_enddate,
            erasure_start: erasure_start,
            model: model,
            manufacturer: manufacturer,
            status: 'IN STOCK',
            hdd: field["cewm.cemd.report.device.memory.internal"]?.replace(".0", "GB"),
            comments: field["cewm.cemd.report.document.custom.field4"],
            grade: grade,
            project_id: field["cewm.cemd.report.document.custom.field1"] || null,
            device_internal_memory: `${field["cewm.cemd.report.device.memory.internal"]}`,
            device_operating_system: field["cewm.cemd.report.device.os"],
            device_imei: field["cewm.cemd.report.device.imei"],
            serial_number: field["cewm.cemd.report.device.serial.number"],
            chasis_type: "MOBILE DEVICE",
            erasure_status: field["cewm.cemd.report.erasure.status"],
            device_model_number: field["cewm.cemd.report.device.model.number"],
            device_model: model,
            system_manufacturer: manufacturer,
            operator_group_name: field["cewm.cemd.report.document.operator.group.name"],
            software_version: field["cewm.cemd.report.document.software.version"],
            report_operator: field["cewm.cemd.report.document.operator.name"],
            document_id: field["cewm.cemd.report.document.id"],
            asset_id: field["cewm.cemd.report.document.custom.field2"],
            device_battery_health: field["cewm.cemd.report.device.battery.health"],
            erasure_lot_name: field["cewm.cemd.report.document.lot.name"],
            wipe_standard: field["cewm.cemd.report.erasure.pattern"],
            supervised: field["cewm.cemd.report.device.supervised"],
            find_my_device: field["cewm.cemd.report.device.find.my"],

        };
        return updateData;
    }


    //mapped certus from 
    // async function getCertusAssetMapped(field, asset_Id) {
    //     let processor = field.processor.split(";");
    //     let processor1 = ''
    //     let processor2 = ''
    //     let processor3 = ''
    //     let processor4 = ''
    //     if (processor[2]) {
    //         processor1 = processor[2].split(":")[1].trim();
    //     }
    //     if (processor[5]) {
    //         processor2 = ', ' + processor[5].split(":")[1].trim();
    //     }
    //     if (processor[8]) {
    //         processor3 = ', ' + processor[2].split(":")[1].trim();
    //     }
    //     processor = processor1 + processor2 + processor3;
    //     // console.log("processor", processor)
    //     let battery = "";
    //     let battery1 = "";
    //     let battery2 = "";
    //     if (field.battery) {
    //         let battery_split = field.battery.split(
    //             ";"
    //         );
    //         if (battery_split[4]) {
    //             battery1 = battery_split[4].split(":")[1].trim();
    //         }
    //         if (battery_split[5]) {
    //             battery2 = battery_split[5].trim();
    //         }
    //         battery = battery1 + " " + battery2;
    //     }
    //     // console.log(field["cewm.ce.report.document.custom.field5"], "battery", battery)

    //     let memoryMB = 0;
    //     let memoryGB = 0;
    //     let memory = 0;
    //     if (field.system_memory) {
    //         let memory_split = field.system_memory.split(
    //             ";"
    //         );
    //         for (let i = 0; i <= memory_split.length; i++) {
    //             if (memory_split[i] && memory_split[i].includes("MB")) {
    //                 memoryMB += parseInt(memory_split[i].replace("MB", "").trim());
    //             }
    //             if (memory_split[i] && memory_split[i].includes("GB")) {
    //                 memoryGB += parseInt(memory_split[i].replace("GB", "").trim());
    //             }
    //         }
    //         memoryMB = (parseInt(memoryMB) / 1024);
    //         memory = parseInt(memoryMB) + parseInt(memoryGB)
    //     }
    //     // console.log("memory", memory)
    //     // console.log(field["cewm.ce.report.document.custom.field5"], "memory", memory)
    //     let erasure_end = field.erasure_time_end.split(" ");
    //     erasure_end.splice(3 - 1, 1);
    //     let erasure_enddate = erasure_end.join(" ");
    //     let erasure_start = field.erasure_time_start.split(" ");
    //     erasure_start.splice(3 - 1, 1);
    //     erasure_start = erasure_start.join(" ");
    //     let status = null;
    //     let grade = null
    //     if (field.grade) {
    //         grade = field.grade.toUpperCase()
    //     }

    //     if (grade === 'A PLUS') {
    //         grade = 'A+'
    //     } else if (grade === 'A +') {
    //         grade = 'A+'
    //     }
    //     if (grade === 'A' || grade === 'B' || grade === 'C') {
    //         status = 'IN STOCK';
    //     } else if (grade === 'D') {
    //         status = "HARVEST";
    //     } else if (grade === 'E') {
    //         status = "RECYCLED";
    //     } else {
    //         status = 'IN STOCK';
    //     }
    //     let dataDestruction = field.erasure_status.toLowerCase();
    //     let complaints = field.complaint.toUpperCase();
    //     if (
    //         dataDestruction === "erasure in progress"
    //         || dataDestruction === "not erased/not erased/not erased"
    //         || dataDestruction === "not erased/not erased"
    //         || dataDestruction === "failed sectors"
    //         || dataDestruction.includes("erased with warning(s) (reallocated sectors not erased: ")
    //         || dataDestruction.includes("erased with warnings (reallocated sectors not erased: ")
    //         || dataDestruction.includes("not erased (")) {
    //         status = 'NOT ERASED';
    //         if (complaints.includes("HDD REM")) {
    //             status = 'IN STOCK';
    //         }
    //     }
    //     let model = field.system_model;
    //     let form_factor = field.chasis_type;
    //     let manufacturer = field.system_manufacturer;
    //     if (manufacturer && manufacturer.toUpperCase() === "HEWLETT-PACKARD") {
    //         manufacturer = "HP";
    //     }
    //     let Part_No = null
    //     if (manufacturer?.toUpperCase() === 'LENOVO') {
    //         Part_No = model.split(' ').pop();
    //         model = field.model_lenova
    //     }

    //     let updateData = {
    //         asset_id: asset_Id,
    //         "Part_No": Part_No,
    //         erasure_with_warning: field.erasure_with_warning,
    //         erasure_lot_name: field.erasure_lot_name,
    //         operator_name: field.operator_name,
    //         manufacturer: manufacturer,
    //         asset_type: "COMPUTER",
    //         quantity: 1,
    //         model: model,
    //         form_factor: form_factor,
    //         processor: processor,
    //         memory: `${memory} GB`,
    //         system_memory: field.memory || "",
    //         hdd: field.device_size.replace(".0", "GB"),
    //         optical: field.optical_drive || "",
    //         graphic_card: field.graphic_card,

    //         battery: battery,
    //         keyboard: field.keyboard,
    //         // pallet_number: "",
    //         serial_number: field.serial_number || '',
    //         hdd_serial_number:
    //             field.device_serial_number || '',
    //         data_destruction: field.erasure_status,
    //         wipe_standard: field.easure_pattern,
    //         erasure_ended: erasure_enddate,
    //         // previous_erasure_ended: new Date(erasure_enddate),
    //         grade: grade,
    //         complaint: complaints,
    //         project_id: field.project_id || null,
    //         status: status,
    //         sample_co2: 55,
    //         sample_weight: 2.0,
    //         erasure_start: erasure_start,
    //         data_generated: "CERTUS"
    //     };
    //     return updateData;
    // }

};