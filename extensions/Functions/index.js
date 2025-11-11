let moment = require('moment')
let request = require('request');
const Excel = require('exceljs');
let _ = require('underscore')

const mailbox = {

    //New Trigger - closing project
    MAILFORCLOSINGPROJECT: async (mailService, data) => {
        let user_created = data.user_created.email
        let sales_ref = data.sales_ref ? data.sales_ref.email : null
        let emails = []
        if (user_created) {
            // if (user_created === 'superadmin@techkonsult.se' || user_created === 'anandsdn@gmail.com') {
            //     user_created = 'roger.persson@techkonsulter.se'
            // }
            emails.push(user_created)
        } if (sales_ref) {
            emails.push(sales_ref)
        }
        try {
            let mailto = emails;
            // var mailto = [  "tord.henryson@gmail.com",
            // "tord.henryson@techauction.se",
            // "tord.henryson@techkonsulter.se","anandsdn@gmail.com"];
            //mail content
            var htmldata = `<div>\
            <p>Project <b>${data.id} </b> from <b>${data.client ? data.client?.client_name : '-'} / ${data.partner ? data.partner.partner_name : '-'} </b> have had products available for sale since <b>${data.process_start_date}</b></p></br>\
            <p> Based on that its time to close project
            </p></br>\
            </div>`;
            var mailOptions = {
                to: mailto,
                subject: `Project ${data.id} should be closed`,
                html: htmldata,
            };
            await mailService.send({ ...mailOptions });
            let obj = {
                status: 'Success',
                emails: emails,
                content: htmldata

            }
            return obj;
        } catch (e) {
            console.log("error =>", e);
            let obj = {
                status: 'Error',
                emails: emails
            }
            return obj;
        }

    },
    CRONJOBS: async (cronService, data, type) => {

        try {
            let date = moment().format('YYYY-MM-DD')
            const crondata = await cronService.readByQuery({
                fields: ["id"],
                filter: {
                    "date": {
                        _eq: date
                    },
                    type: {
                        _eq: type
                    },
                },
            });
            let obj = {
                type: type,
                totalassets: JSON.parse(data).length,
                certus_values: JSON.stringify(data)
            };

            if (crondata?.length > 0) {
                await cronService.updateOne(crondata[0].id, obj
                )
            } else {
                await cronService.createOne(
                    obj
                )

            }
            // return true;
        } catch (e) {
            return false;
        }

    },
    UPDATEPROJECTFINANCE: async (project_id, projectService, database, res) => {

        try {
            project_id = typeof project_id === 'object' ? project_id.id : project_id;
            if (!project_id) {
                return
            }
            let sql1 = `select order_commission,order_revenue,commision_percentage,revenue,remarketing,software,handling,logistics,buyout,other,invoice_by_client,invoice_rec_amount from public."project" where id = ${project_id} and project_status !='CLOSED' and  status !='archived'`;

            await database.raw(sql1)
                .then(async (projRes) => {
                    let projectValues = projRes.rows[0];
                    console.log("prj values", projectValues)
                    let values1 = {};
                    let types = ["remarketing", "logistics", "handling", "software", "buyout", "other"];
                    let sql1 = `select lower(type) as type,sum(total) as total from public."project_finance" where project_id = ${project_id} and status !='archived' group by lower(type)`;
                    await database.raw(sql1)
                        .then(async (response1) => {
                            if (response1.rows?.length > 0) {
                                let result = response1.rows;
                                // console.log("result", result)
                                result.forEach((item, i) => {
                                    if (types.includes(item.type)) {
                                        values1[item.type] = item.total
                                    }
                                })
                            }
                        })
                    let remarketing = 0;
                    let buyout = values1?.buyout ? Number(values1.buyout) : 0
                    let logistics = values1?.logistics ? Number(values1.logistics) : 0
                    let handling = values1?.handling ? Number(values1.handling) : 0
                    let other = values1?.other ? Number(values1.other) : 0
                    let software = projectValues.software || 0;
                    let commision_percentage = projectValues.commision_percentage;
                    // let order_commission = 0
                    // if (projectValues.commision_percentage) {
                    // 	order_commission = parseFloat(projectValues.commision_percentage) + 5;
                    // }
                    // let order_commission = 0
                    // if (Number(projectValues.commision_percentage)) {
                    //   order_commission = projectValues.order_commission = parseFloat(projectValues.commision_percentage) + 5;
                    // } else if (!projectValues.commision_percentage) {
                    //   order_commission = projectValues.order_commission = 15;
                    // }

                    let order_commission = 0
                    if (Number(projectValues.order_commission)) {
                        order_commission = projectValues.order_commission
                    } else if (!projectValues.commision_percentage && projectValues.order_commission) {
                        order_commission = projectValues.order_commission;
                    } else if (!projectValues.commision_percentage && !projectValues.order_commission) {
                        order_commission = projectValues.order_commission = 15
                    }

                    let order = 0
                    if (projectValues.order_revenue) {
                        order = parseFloat(projectValues.order_revenue);
                    }
                    let order_commission_value = 0
                    if (order && order_commission) {
                        order_commission_value = Math.round((order / 100) * order_commission)
                    }
                    let commision = Math.round((projectValues.revenue / 100) * commision_percentage) + order_commission_value;
                    // remarketing = Math.round(projectValues.revenue - commision);
                    remarketing = Math.round(parseFloat(projectValues.revenue) + parseFloat(order) + parseFloat(buyout) - commision);

                    let invoice_by_client = 0;
                    // console.log("**************", values)
                    invoice_by_client = parseFloat(remarketing) - parseFloat(logistics) - parseFloat(handling) - parseFloat(software) - parseFloat(other);
                    // console.log("invoice_by_client",invoice_by_client)
                    let difference = 0;
                    if (projectValues.invoice_rec_amount) {
                        difference = parseFloat(invoice_by_client) - parseFloat(projectValues.invoice_rec_amount);
                    }
                    let obj = {
                        logistics: logistics,
                        handling: handling,
                        buyout: buyout,
                        other: other,
                        software: software,
                        invoice_by_client: invoice_by_client,
                        remarketing: remarketing,
                        order_commission: order_commission,
                        commision: commision,
                        difference: difference
                    }
                    // console.log("objjjjjjjjj", obj)
                    return await projectService.updateOne(project_id,
                        obj
                    ).then((response1) => {
                        // res.json(response);
                        console.log("project updateee success", response1)
                        // res.send({
                        //     status: 200,
                        //     msg: 'Updated project finance'
                        // })
                        return true
                        // res.status(200).send({
                        //     msg: 'Updated project finance'
                        //   })

                    }).catch((error1) => {
                        console.log("project finance  updateee errrrr", error1)
                        return false;
                    });
                })
            // return true;
        } catch (e) {
            return false;
        }

    },
    UPDATECOMPLIANTS: async (data, service) => {
        try {
            if (data.selected_complaints_id) {
                let ids = data.selected_complaints_id.split(',');

                if (ids) {
                    for (let i = 0; i < ids.length; i++) {
                        const result = await service.readByQuery({
                            fields: ["id", "count", "sorting"],
                            filter: {
                                id: {
                                    _eq: ids[i]
                                },
                            },
                        });

                        if (data.complaint_type !== 'all') {
                            let sorting = result[0].sorting;
                            sorting[data.complaint_type] = Number(sorting[data.complaint_type]) + 1;
                            await service.updateOne(result[0].id,
                                {
                                    sorting: sorting
                                }
                            )
                        } else {
                            await service.updateOne(result[0].id,
                                {
                                    count: result[0].count + 1
                                }
                            )
                        }

                    }

                }
            }
            // return true;
        } catch (e) {
            return false;
        }

    },
    CERTUSMOBILEREQUEST: () => {
        return [
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
    },
    MAPCERTUS: async (field) => {
        let erasure_end = field["cewm.ce.report.erasure.time.end"].split(" ");
        erasure_end.splice(3 - 1, 1);
        let erasure_enddate = erasure_end.join(" ");

        const certusdata = {
            erasure_ended: erasure_enddate,
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
    },
    MAP_CERTUS_TO_ASSET: async (field) => {
        let processor = field["cewm.ce.report.hardware.system.processor"].split(";");
        let processor1 = ''
        let processor2 = ''
        let processor3 = ''
        let processor4 = ''
        if (processor[2]) {
            processor1 = processor[2].split(":")[1].trim();
        }
        if (processor[5]) {
            processor2 = ', ' + processor[5].split(":")[1].trim();
        }
        if (processor[8]) {
            processor3 = ', ' + processor[2].split(":")[1].trim();
        }
        processor = processor1 + processor2 + processor3;
        // console.log("processor", processor)
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
        let status = '';
        let grade = ''
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
            || dataDestruction === "not erased"
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
            memory: `${memory} GB`,
            system_memory: field["cewm.ce.report.hardware.system.memory"] || null,
            hdd: field["cewm.ce.report.erasure.device.size"].replace(".0", "GB"),
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
            status: status,
            sample_co2: 55,
            sample_weight: 2.0,
            erasure_start: erasure_start,
            data_generated: "CERTUS"
        };
        return updateData;
    },
    CREATEACCESS: async (data, projectService, usersservice) => {
        try {
            let clientData
            if (data.client) {
                clientData = await usersservice.readByQuery({
                    fields: ["id",],
                    filter: {
                        client: {
                            _eq: data.client
                        }
                    },
                });
            }
            let partnerData
            if (data.partner) {
                partnerData = await usersservice.readByQuery({
                    fields: ["id",],
                    filter: {
                        partner: {
                            _eq: data.partner
                        },
                        isDefault: {
                            _eq: true
                        }
                    },
                });
            }
            let clientAccess = []
            if (clientData?.length > 0) {
                clientAccess = clientData.map((itm) => itm.id);
            }
            let partnerAccess = []
            if (partnerData?.length > 0) {
                partnerAccess = partnerData.map((itm) => itm.id);
            }
            let accessIds = [...clientAccess, ...partnerAccess]
            let access = []
            let actions = {};
            let obj = {};
            if (accessIds?.length > 0) {
                for (let i = 0; i < accessIds.length; i++) {
                    let val = {
                        project_id: data.id,
                        project_users_id: {
                            id: accessIds[i],
                        },
                    };
                    access.push(val);
                }
                actions.create = access;
                obj.product_report = actions;
                obj.tk_access = actions;
                obj.financial_access = actions;
                await projectService.updateOne(data.id, obj);
            }


        } catch (e) {
            console.log("error", e)
            return next(new ServiceUnavailableException(e));
        }
    },
    NERDFIXTRANSPORT: async (data, assetsService, nerdfixservice, database, date, ServiceUnavailableException) => {
        try {
            const assetData = await assetsService.readByQuery({
                fields: ["asset_id", "asset_id_nl", "sold_order_nr", "project_id"],
                filter: {
                    asset_id: {
                        _eq: data.asset_id
                    }
                },
            });
            if (assetData?.length > 0 && assetData[0]?.asset_id_nl && assetData[0]?.sold_order_nr) {
                // const nerdfixHistory = await nerdfixservice.readByQuery({
                //     fields: ["asset_id_nl"],
                //     filter: {
                //         asset_id_nl: {
                //             _eq: assetData[0].asset_id_nl
                //         },
                //         api_request: {
                //             _eq: 'TRANSPORT_API'
                //         }
                //     },
                // });

                // if (nerdfixHistory?.length === 0) {
                let formdata = {
                    products: [assetData[0].asset_id_nl],
                    sold_order_nr: assetData[0].sold_order_nr
                }
                var options = {
                    'method': 'POST',
                    'url': 'https://app.nerdfix.nl/api/v1/transport',
                    'headers': {
                        'Authorization': 'bee29a6816c41a338cfe50d0c67980c6b26d18dc797506becd39c7b3f3b13d4a',
                        'Cookie': 'PHPSESSID=uv9rv3vla7l4o4ojeqheadlmra'
                    },
                    formData: {
                        'data': `${JSON.stringify(formdata)}`
                    }
                };
                request(options, async function (error, response) {
                    if (error) {
                        console.log("errrror nerdix")
                        throw new Error(error);
                    }
                    else {
                        if (assetData[0].asset_id_nl) {
                            let obj = {
                                asset_id_nl: assetData[0].asset_id_nl,
                                asset_status: 'RESERVATION',
                                api_response: JSON.stringify(response.body),
                                api_request: 'TRANSPORT_API',
                                api_params: JSON.stringify(formdata)
                            }
                            await nerdfixservice.createOne(
                                obj
                            ).then((response1) => {
                                // res.json(response);
                                console.log("nerdfix history create success")
                            }).catch((error1) => {
                                console.log("nerdfix history failed to create", error1)
                            });

                            // let insertsql = `insert into public."nerdfixs_history" (asset_id_nl,asset_status,date_created,api_response,api_request,api_params) values (${assetData[0].asset_id_nl},'RESERVATION','${moment()}', '${JSON.stringify(response.body)}','TRANSPORT_API','${JSON.stringify(formdata)}')`;
                            // await database.raw(insertsql)
                            //     .then(async (response) => {
                            //         console.log("create nerdfix")
                            //     })
                            //     .catch((error) => {
                            //         console.log(insertsql, "insert nerdfixerrrr", error)
                            //     });
                        }

                    }
                });
                // }

            }

        } catch (e) {
            console.log("error", e)
            // return next(new ServiceUnavailableException(e));
        }
    },
    UPDATENERDFIXPRODUCT: async (data, assetsService, ServiceUnavailableException, database, API_ACTION) => {
        try {
            const assetData = await assetsService.readByQuery({
                fields: ["asset_id_nl", "project_id", "asset_type", "form_factor", "Part_No", "quantity", "manufacturer", "model", "imei", "serial_number", "processor", "memory", "hdd", "hdd_serial_number", "hdd_count", "optical", "graphic_card", "battery", "keyboard", "screen", "description", "data_generated", "created_at", "operator_name", "grade", "erasure_ended", "erasure_lot_name", "data_destruction", "complaint", "complaint_1", "wipe_standard", "erasure_with_warning", "supervised", "find_my_device", "status"],
                filter: {
                    asset_id: {
                        _eq: data.asset_id
                    }
                },
            });
            if (assetData?.length > 0 && assetData[0]?.asset_id_nl) {
                let obj = assetData[0];
                obj.asset_id = obj.asset_id_nl
                let formdata = obj;
                delete formdata.asset_id_nl
                // delete formdata.asset_id
                var options = {
                    'method': 'POST',
                    'url': 'https://app.nerdfix.nl/api/v1/product',
                    'headers': {
                        'Authorization': 'bee29a6816c41a338cfe50d0c67980c6b26d18dc797506becd39c7b3f3b13d4a',
                        'Cookie': 'PHPSESSID=uv9rv3vla7l4o4ojeqheadlmra'
                    },
                    formData: {
                        'data': `${JSON.stringify(formdata)}`
                    }
                };
                // console.log("formdata", formdata)
                // return
                request(options, async function (error, response) {
                    if (error) {
                        if (obj.asset_id) {
                            let insertsql = `insert into public."nerdfixs_history" (asset_id_nl,asset_status,date_created,api_response,api_request,api_params) values (${obj.asset_id},'${obj.status}-errr','${moment().format('YYYY-MM-DD hh:mm')}', '${JSON.stringify(error)}','${API_ACTION}','${JSON.stringify(formdata)}')`;
                            await database.raw(insertsql)
                                .then(async (response1) => {
                                    console.log("create nerdfix")
                                })
                                .catch((error) => {
                                    console.log("update product - insert nerdfixerrrr", error)
                                });
                        }
                        throw new Error(error);
                    }
                    else {
                        console.log("update nerdfixproduct success");
                        if (obj.asset_id) {
                            let insertsql = `insert into public."nerdfixs_history" (asset_id_nl,asset_status,date_created,api_response,api_request,api_params) values (${obj.asset_id},'${obj.status}','${moment().format('YYYY-MM-DD hh:mm')}', '${JSON.stringify(response.body)}','${API_ACTION}','${JSON.stringify(formdata)}')`;
                            await database.raw(insertsql)
                                .then(async (response2) => {
                                    console.log("create nerdfix")
                                })
                                .catch((error) => {
                                    console.log(insertsql, "insert nerdfixerrrr", error)
                                });
                        }
                    }
                });
            }

        } catch (e) {
            console.log("error", e)
            throw new ServiceUnavailableException(e);
        }
    },
    SENDMAILLOCKEDDEVICES: async (database, project_id = null, mailService, lockeddevices) => {
        try {
            // console.log("lockeddevices", lockeddevices[0]?.project_id?.contact_attn?.email)
            let emails = [];
            let client_name = '';

            if (lockeddevices[0]?.project_id?.contact_attn?.email) {
                emails.push(lockeddevices[0]?.project_id?.contact_attn?.email)
            }
            if (lockeddevices[0]?.project_id?.client?.client_name) {
                client_name = lockeddevices[0]?.project_id?.client?.client_name;
            }
            // let emails = ['anandsdn@gmail.com'];
            let body = ``;
            let html = ``
            let subject = ``;
            let content = ``
            let mail_status = ''
            let htmlTable = ''
            if (project_id) {
                let footer = `<span>Best regards</span><br/><span>Itreon</span><br/><span>support@itreon.se</span>`
                mail_status = 3;
                subject = `Locked devices in Itreon project ${project_id} has been found`
                body = `<span>Hi,</span><br/>We want to inform you that in project <strong>${project_id}</strong> for client <strong>${client_name}</strong> locked devices has been found so we ask you to urgently unlock them. <br/>Locked devices cant be reused and will result in less value in project overall and waste of otherwise reuseable equipment.`;
                body = `${body} <br/> Please come back as soon as possible, if we don’t hear from you within 5 days we will proceed project without unlocking the devices.<br/><br/>${footer}`
                body = `${body} <br/><br/> Below is list of devices with serial/IMEI that are reported locked:<br/><br/>`

                htmlTable = "<table class='table table-hover' style='width: 43%'>" +
                    "<thead>" +
                    "<tr>" +
                    "<th style='text-align: left;'>Model</th>" +
                    "<th style='text-align: left;'>Serial number</th>" +
                    "<th style='text-align: left;'>IMEI</th>" +
                    "</tr>" +
                    "</thead>" +
                    "<tbody>"
                lockeddevices.forEach((value, key) => {
                    // console.log("valuesssssss", value)
                    htmlTable +=
                        "<tr>" +
                        "<td scope='row'>" + (value.model || '') + "</td>" +
                        "<td>" + (value.serial_number || '') + "</td>" +
                        "<td>" + (value.imei || '') + "</td>" +
                        "</tr>"
                })
                htmlTable +=
                    +"</tbody>" +
                    "</table>"
                body = `${body}<br/>${htmlTable}`
                content = `${body}`
                emails.forEach(async (email) => {
                    try {
                        if (email) {
                            // await mailService.send({
                            //     to: [email],
                            //     subject: subject,
                            //     html: content,
                            // });
                        }
                    } catch (error) {
                        console.log("error", error)
                        // throw new ServiceUnavailableException(error);
                    }

                })

                let sql3 = `select delievery_address,sub_org,address_info,a.postal_code,a.city,a.country, c.client_name,p.client_ref from public.clients_address a, public.project p, public.clients c where p.id = ${project_id} and p.delivery_address = a.id and c.id = p.client`;
                await database.raw(sql3)
                    .then(async (response) => {
                        // console.log("responseeeeeeeeeeee", response)
                        let result = response.rows[0];
                        client_name = result?.client_name || '';


                        return;
                        //delivery_address = `${result?.client_name ? '<span>' + result?.client_name + '</span><br/>' : ''}${result?.delievery_address ? '<span>' + result?.delievery_address.replace(/ *, */g, '<br>') + '</span><br/>' : ''} ${result?.sub_org ? '<span>' + result?.sub_org + '</span><br/>' : ''}${result?.address_info ? '<span>' + result?.address_info + '</span><br/>' : ''}${result?.postal_code ? '<span>' + result?.postal_code + '</span><br/>' : ''}${result?.city ? '<span>' + result?.city + '</span><br/>' : ''} ${result?.country ? '<span>' + result?.country + '</span>' : ''}`
                        let sql2 = `SELECT distinct b.email FROM public.project_tk_users a, public.directus_users b where a.project_users_id = b.id and project_id = ${project_id}`;
                        await database.raw(sql2)
                            .then(async (response) => {
                                let res = response.rows;
                                emails = res.map(
                                    (item) => item.email
                                );
                                if (emails?.length === 0) {
                                    return
                                }

                                let footer = `<span>Best regards</span><br/><span>Itreon</span><br/><span>support@itreon.se</span>`
                                mail_status = 3;
                                subject = `Locked devices in Itreon project ${project_id} has been found`
                                body = `<span>Hi,</span><br/>We want to inform you that in project <strong>${project_id}</strong> for client <strong>${client_name}</strong> locked devices has been found so we ask you to urgently unlock them. <br/>Locked devices cant be reused and will result in less value in project overall and waste of otherwise reuseable equipment.`;
                                body += `Below is list of devices with serial/IMEI that are reported locked:`
                                html = `<table style="font-size: 14px;width:100%"></tr>${client_name ? '<tr><td style="font-weight: 600;">Model</td></tr>' : ''}<tr><td style="font-weight: 600;">${body}</td><tr><td style="font-weight: 600;"><br/>Please contact us if you wonder anything about your project!</td></tr></tr></table>`
                                content = `<br/><br/>${html}`
                                content = `${content}<br/>${footer}`
                                emails.forEach(async (email) => {
                                    try {
                                        if (email) {
                                            await mailService.send({
                                                to: [email],
                                                subject: subject,
                                                html: content,
                                            });
                                        }
                                    } catch (error) {
                                        console.log("error", error)
                                        // throw new ServiceUnavailableException(error);
                                    }

                                })

                            })
                            .catch((error) => {
                                // res.send(500)
                            });


                    })
                    .catch((error) => {
                        // res.send(500)
                    });

            }
        } catch (error) {
            console.log("error 111", error)
            // throw new ServiceUnavailableException(error);
        }
    },
    MAPPRODUCTREPORT: async (asset_lists) => {
        try {
            let excelcolumns = [
                { header: 'Project ID', key: 'project_id' },
                { header: 'Asset ID', key: 'asset_id' },
                { header: 'Asset type', key: 'asset_type' },
                { header: 'Form factor', key: 'form_factor' },
                { header: 'Part Number', key: 'Part_No' },
                { header: 'QTY', key: 'quantity' },
                { header: 'Manufacturer', key: 'manufacturer' },
                { header: 'Model', key: 'model' },
                { header: 'IMEI', key: 'imei' },
                { header: 'Serial', key: 'serial_number' },
                { header: 'processor', key: 'processor' },
                { header: 'memory', key: 'memory' },
                { header: 'HDD', key: 'hdd' },
                { header: 'HDD SERIAL NUMBER', key: 'hdd_serial_number' },
                { header: 'Optical', key: 'optical' },
                { header: 'Graphic card', key: 'graphic_card' },
                { header: 'Battery', key: 'battery' },
                { header: 'Keyboard', key: 'keyboard' },
                { header: 'Screen', key: 'screen' },
                { header: 'Grade', key: 'grade' },
                { header: 'Complaint', key: 'complaint' },
                { header: 'Erasure ended', key: 'erasure_ended' },
                { header: 'Data destruction', key: 'data_destruction' },
                { header: 'Wipe standard', key: 'wipe_standard' },
                { header: 'Sample co2', key: 'sample_co2' },
                { header: 'Sample weight', key: 'sample_weight' },
                { header: 'Client REFERENCE', key: 'client_ref' },
                { header: 'Client name', key: 'client_name' },
                { header: 'SUBORG NAME', key: 'suborg_name' }
            ];

            asset_lists.forEach((obj) => {
                if (obj.manufacturer && obj.manufacturer.toUpperCase() === "HEWLETT-PACKARD") {
                    obj.manufacturer = "HP";
                }
                //--------------------sameple co2
                // let form_factor = null
                // let currentAssetType = ''
                // let asset_type = obj?.asset_type ? obj.asset_type.trim().toLowerCase() : null;
                // if (asset_type) {
                //     if (obj.form_factor && (obj.form_factor !== '' || obj.form_factor !== null)) {
                //         form_factor = obj.form_factor.trim().toLowerCase()
                //         currentAssetType = assetTypesList.filter(
                //             (asset) => ((asset.Asset_Name.toLowerCase() === asset_type) && asset.formfactor?.trim().toLowerCase() === form_factor)
                //         );
                //     } else {
                //         currentAssetType = assetTypesList.filter(
                //             (asset) => ((asset.Asset_Name.toLowerCase() === asset_type) && (asset.formfactor === null || asset.formfactor === ''))
                //         );
                //     }
                //     if (currentAssetType?.length > 0) {
                //         obj.sample_co2 = ((obj.quantity || 1) * Number(currentAssetType[0]?.sampleco2)) || '';
                //         obj.sample_weight = ((obj.quantity || 1) * Number(currentAssetType[0]?.sample_weight)).toFixed(1) || '';
                //     }
                // }
                //--------------------------------

                if (obj.asset_type && (obj.asset_type.toUpperCase() === 'COMPUTER')) {
                    let complaint = obj?.complaint ? obj.complaint.toLowerCase() : null
                    let complaints_1 = obj?.complaints_1 ? obj.complaints_1.toLowerCase() : null
                    if ((complaint && complaint.includes('no ram')) || (complaints_1 && complaints_1.includes('no ram'))) {
                        obj.memory = 'N/A'
                    }
                }
                if (obj.graphic_card) {
                    if (obj.graphic_card && (obj.data_generated === 'CERTUS')) {
                        var text = obj.graphic_card
                        var regex = /\[([^\][]*)]/g;
                        var results = [], m;
                        while (m = regex.exec(text)) {
                            results.push(m[1]);
                        }
                        obj.graphic_card = results ? results.toString() : '';
                    }
                }
                if (obj.asset_type && (obj.asset_type.toUpperCase() === 'COMPUTER')) {
                    let hddtext = ["no hdd", "hdd rem", "hdd crash", "hdd fail"]
                    let complaint = obj?.complaint ? obj.complaint.toLowerCase() : null
                    let complaints_1 = obj?.complaints_1 ? obj.complaints_1.toLowerCase() : null;
                    let isComplaintTrue = false;
                    let isComplaint_1True = false;
                    hddtext.forEach((item) => {
                        if (complaint && complaint.includes(item)) {
                            isComplaintTrue = true;
                        }
                        if (complaints_1 && complaints_1.includes(item)) {
                            isComplaint_1True = true;
                        }
                    })
                    if (obj.hdd && (obj.hdd !== 'N/A' && !obj.hdd.includes('/') && (isComplaintTrue || isComplaint_1True))) {
                        obj.hdd = 'N/A'
                    }
                    else if (!obj.hdd && (isComplaintTrue || isComplaint_1True)) {
                        obj.hdd = 'N/A'
                    }
                }

            })
            let workbook = new Excel.Workbook();
            let worksheet = workbook.addWorksheet();
            worksheet.columns = excelcolumns;
            let header = [];
            let headerName = [];
            excelcolumns.map((val) => {
                let width = 14
                if (val.key === 'model') {
                    width = 40
                }
                header.push({ key: val.key, width: width })
                headerName.push(val.header.toUpperCase())
            })
            worksheet.getRow(3).values = ['           Product report'];
            worksheet.getRow(4).values = [`           Client: ${asset_lists[0].client_name || ''}`];
            worksheet.getRow(5).values = [`           Project number: ${asset_lists[0].project_id || ''}`];
            worksheet.getRow(8).values = headerName;
            ['A1', 'B1', 'C1', 'D1', 'E1', 'F1', 'A2', 'B2', 'C2', 'D2', 'E2', 'F2', 'A3', 'B3', 'C3', 'D3', 'E3',
                'F3', 'A4', 'B4', 'C4', 'D4', 'E4', 'F4', 'A5', 'B5', 'C5', 'D5', 'E5', 'F5', 'A6', 'B6', 'C6', 'D6', 'E6', 'F6', 'A7', 'B7', 'C7', 'D7', 'E7', 'F7']
                .map(key => {
                    worksheet.getCell(key).fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: '004750' },
                        color: { argb: 'FFFFFF' },
                        bgColor: { argb: '004750' }
                    };
                });

            ['G1', 'H1', 'I1', 'J1', 'K1', 'L1', 'M1', 'N1', 'O1', 'P1', 'Q1', 'R1', 'S1', 'T1', 'U1', 'V1', 'W1', 'X1', 'Y1', 'Z1', 'AA1', 'AB1', 'AC1',
                'G2', 'H2', 'I2', 'J2', 'K2', 'L2', 'M2', 'N2', 'O2', 'P2', 'Q2', 'R2', 'S2', 'T2', 'U2', 'V2', 'W2', 'X2', 'Y2', 'Z2', 'AA2', 'AB2', 'AC2',
                'G3', 'H3', 'I3', 'J3', 'K3', 'L3', 'M3', 'N3', 'O3', 'P3', 'Q3', 'R3', 'S3', 'T3', 'U3', 'V3', 'W3', 'X3', 'Y3', 'Z3', 'AA3', 'AB3', 'AC3',
                'G4', 'H4', 'I4', 'J4', 'K4', 'L4', 'M4', 'N4', 'O4', 'P4', 'Q4', 'R4', 'S4', 'T4', 'U4', 'V4', 'W4', 'X4', 'Y4', 'Z4', 'AA4', 'AB4', 'AC4',
                'G5', 'H5', 'I5', 'J5', 'K5', 'L5', 'M5', 'N5', 'O5', 'P5', 'Q5', 'R5', 'S5', 'T5', 'U5', 'V5', 'W5', 'X5', 'Y5', 'Z5', 'AA5', 'AB5', 'AC5',
                'G6', 'H6', 'I6', 'J6', 'K6', 'L6', 'M6', 'N6', 'O6', 'P6', 'Q6', 'R6', 'S6', 'T6', 'U6', 'V6', 'W6', 'X6', 'Y6', 'Z6', 'AA6', 'AB6', 'AC6',
                'G7', 'H7', 'I7', 'J7', 'K7', 'L7', 'M7', 'N7', 'O7', 'P7', 'Q7', 'R7', 'S7', 'T7', 'U7', 'V7', 'W7', 'X7', 'Y7', 'Z7', 'AA7', 'AB7', 'AC7']
                .map(key => {
                    worksheet.getCell(key).fill = {
                        type: 'pattern',
                        pattern: 'solid',
                        fgColor: { argb: 'FFFFFF' },
                        color: { argb: 'FFFFFF' },
                        bgColor: { argb: 'FFFFFF' }
                    };
                });

            worksheet.columns = header // Asign header
            worksheet.columns.forEach(column => {
                column.width = column.width
            })
            asset_lists.forEach(function (item, index) {
                worksheet.getRow(index + 8).height = 24;
                worksheet.addRow(item)
            })
            worksheet.eachRow((row, rowNumber) => {
                row.eachCell((cell) => {
                    if (rowNumber == 3 || rowNumber == 4 || rowNumber == 5) {
                        cell.font = {
                            // bold: true,
                            color: { argb: 'FFFFFF' }, // white text
                            size: 12, // Font size 14 for header
                            name: 'Work Sans' // Custom font family for header
                        };
                    } else if (rowNumber == 8) {
                        if (cell._address === "F8") {
                            cell.alignment = { horizontal: 'center', wrapText: true };
                        }
                        cell.fill = {
                            type: 'pattern',
                            pattern: 'solid',
                            color: { argb: 'FFFFFF' },
                            bgColor: { argb: 'BBC6C3' },
                            fgColor: { argb: 'BBC6C3' }
                        };
                        cell.font = {
                            color: { argb: '004750' }, // white text
                            size: 12, // Font size 14 for header
                            name: 'Work Sans', // Custom font family for header

                        };
                    } else {
                        cell.alignment = { wrapText: true };
                        cell.height = 40;
                        cell.font = {
                            size: 10, // Font size 14 for header
                            name: 'Work Sans' // Custom font family for header
                        };
                    }

                })
            })
            worksheet.spliceRows(0, 1)
            let buffer = await workbook.xlsx.writeBuffer();
            return buffer
        } catch (err) {
            throw err
        }
    },
    NERDFIXBULKUPDATE: async (assetData, nerdfixservice, index) => {
        try {
            if (assetData?.products?.length > 0) {
                var options = {
                    'method': 'POST',
                    'url': 'https://app.nerdfix.nl/api/v1/transport',
                    'headers': {
                        'Authorization': 'bee29a6816c41a338cfe50d0c67980c6b26d18dc797506becd39c7b3f3b13d4a',
                        'Cookie': 'PHPSESSID=uv9rv3vla7l4o4ojeqheadlmra'
                    },
                    formData: {
                        'data': `${JSON.stringify(assetData)}`
                    }
                };
                let val = true
                return await request(options, async function (error, response) {
                    if (error) {
                        console.log("errrror nerdix")
                        throw new Error(error);
                    }
                    else {
                        let obj = {
                            asset_status: 'RESERVATION',
                            api_response: JSON.stringify(response.body),
                            api_request: 'TRANSPORT_API',
                            api_params: JSON.stringify(assetData)
                        }
                        await nerdfixservice.createOne(
                            obj
                        ).then((response1) => {
                            console.log("nerdfix bulk update history create success");
                            val = true
                        }).catch((error1) => {
                            console.log("nerdfix history failed to create", error1);
                            val = false;
                        });
                        return val

                    }
                });
            }
        } catch (e) {
            console.log("error", e)
            // return next(new ServiceUnavailableException(e));
        }
    },
    UPDATEPROCESSEDUPDATE: async (project_id, database) => {
        try {
            let update_processed_units_sold_sql = `UPDATE public.project a
            SET processed_units_sold=ROUND((subquery.processed_units_sold::NUMERIC / no_of_assets_1::NUMERIC) *100)
            FROM (select sum(quantity) as processed_units_sold,project_id from public."Assets" where LOWER(asset_type) not in ('adapter', 'cable') and LOWER(status) in ('sold', 'reservation') GROUP BY project_id) AS subquery
            WHERE id = subquery.project_id and no_of_assets_1::int >0`;
            await database.raw(update_processed_units_sold_sql).then(async (response) => {
                console.log("processed_units_sold ===>", project_id)
            }).catch((error) => {
                console.log("error", error)
            });

        } catch (e) {
            console.log("error", e)
            // return next(new ServiceUnavailableException(e));
        }
    },
    UPDATEPROJECTSQL: async (project_id, database) => {
        try {
            let totalAssets_sql = `select sum(quantity) from public."Assets" where project_id = ${project_id}`;
            await database.raw(totalAssets_sql)
                .then(async (totalAssets_response) => {
                    if (totalAssets_response.row?.length === 0) {
                        let set_no_of_assets_empty = `UPDATE public.project a
                            SET processed_units_sold=0, no_of_assets=0
                            WHERE id =${project_id}`;
                        await database.raw(set_no_of_assets_empty).then(async (set_no_of_assets_response) => {
                            console.log("processed_units_sold ===>", project_id)
                        }).catch((error) => {
                            console.log("error", error)
                        });

                    } else {
                        let update_no_of_assets_sql = `UPDATE public.project a
                            SET no_of_assets_1=subquery.no_of_assets_1
                            FROM (SELECT sum(quantity) as no_of_assets_1,project_id
                            FROM public."Assets" b 
                            where (LOWER(b.asset_type) not in ('adapter', 'cable') OR LOWER(b.form_factor) not in ('adapter', 'cable'))
                            and project_id = ${project_id}
                            GROUP BY b.project_id) AS subquery
                            WHERE id = subquery.project_id`;
                        await database.raw(update_no_of_assets_sql)
                            .then(async (response) => {
                                // console.log("no of asset updated ==>", project_id)
                                let update_processed_units_sold_sql = `UPDATE public.project a
                                        SET processed_units_sold=ROUND((subquery.processed_units_sold::NUMERIC / no_of_assets_1::NUMERIC) *100)
                                        FROM (select sum(quantity) as processed_units_sold,project_id from public."Assets" where LOWER(asset_type) not in ('adapter', 'cable') 
                                        and LOWER(status) in ('sold', 'reservation')
                                        and project_id = ${project_id}
                                        GROUP BY project_id) AS subquery
                                        WHERE id = subquery.project_id and no_of_assets_1::int >0`;
                                await database.raw(update_processed_units_sold_sql).then(async (response) => {
                                    console.log("processed_units_sold ===>", project_id)
                                }).catch((error) => {
                                    console.log("error", error)
                                });

                            }).catch((error) => {
                                console.log("error==>", error)
                            });
                    }

                }).catch((error) => {
                    console.log("error==>", error)
                });


        } catch (e) {
            console.log("error", e)
            // return next(new ServiceUnavailableException(e));
        }
    },
    UPDATEESTIMATEVALUECOMPUTER: async (field, database, estimate_values_service) => {
        let cond = ''
        if (field) {
            cond = `and UPPER(processor) like '${field.processor}' and UPPER(model) like '${field.model}'`
        }
        let computerResponse = [];
        let results = [];
        let computerResponse1 = [];
        let sql4 = `SELECT
		UPPER(form_factor) form_factor,
		UPPER(model) model,
		UPPER(processor) processor,
		SUM(quantity) AS total_grade_sum,
		string_agg(UPPER(processor)::text, ',') as "computer_info"
		 from
		public."Assets" where grade is not null and UPPER(asset_type) in ('COMPUTER') 
		and UPPER(grade) in ('A','B','C') 
		and LOWER(form_factor) in ('desktop','laptop') 
		and date_nor::date >=  CURRENT_DATE - INTERVAL '2 months'
        and status in ('RESERVATION','SOLD')
		GROUP BY UPPER(model),UPPER(processor),UPPER(form_factor)`;
        await database.raw(sql4)
            .then(async (CompuRes) => {
                computerResponse = CompuRes.rows;

                for (const itm of computerResponse) {
                    // return
                    // last 2 months total sold grade
                    let sql2 = `select UPPER(model) model,UPPER(processor) processor,UPPER(manufacturer) manufacturer,UPPER(form_factor) form_factor,
				string_agg(UPPER(processor)::text, ',') as "computer_info",
				UPPER(grade) grade,
				sum(quantity) sold_qty,sum(sold_price) as tot_soldprice from public."Assets"
				where
				UPPER(asset_type) in ('COMPUTER')
				and UPPER(grade) in ('A','B','C')
				and LOWER(form_factor) in ('desktop','laptop') 
				and (date_nor is not null) and date_nor::date >  CURRENT_DATE - INTERVAL '2 months'
				and LOWER(processor) = '${itm.processor ? itm.processor.toLowerCase() : ''}'
				and LOWER(model) = '${itm.model ? itm.model.toLowerCase() : ''}'
                and status in ('RESERVATION','SOLD')
				group by UPPER(model),UPPER(form_factor),UPPER(manufacturer),UPPER(processor),UPPER(grade) order by UPPER(grade)`
                    await database.raw(sql2)
                        .then(async (CompuRes1) => {
                            computerResponse1 = CompuRes1.rows;

                            let form_factor = itm.form_factor.toLowerCase();
                            if (itm.computer_info && (form_factor === 'desktop' || form_factor === 'laptop')) {
                                itm.computer_info_2 = _.uniq(itm.computer_info.split(','));
                            }

                            for (const itm2 of computerResponse1) {
                                // console.log("itm2",itm2)
                                let avg_prvice = (itm2.tot_soldprice / itm2.sold_qty)
                                itm2.last_60_days_sold = avg_prvice ? Math.round(avg_prvice) : 0 // Grade values
                                itm2.last_60_days_qty_sold = itm2.sold_qty;
                                let form_factor_2 = itm2.form_factor.toLowerCase();

                                if (itm2.computer_info && (form_factor_2 === 'desktop' || form_factor_2 === 'laptop')) {
                                    itm2.computer_info = _.uniq(itm2.computer_info.split(','));
                                }
                                let grade = itm2.grade?.toLowerCase().toString();
                                // let grade_2 = itm2.grade?.toLowerCase().toString();
                                let model = itm.model?.toLowerCase().toString();
                                let model_2 = itm2.model?.toLowerCase().toString();
                                let info = itm2.computer_info?.toString();
                                let info_2 = itm.computer_info_2?.toString();
                                itm2.asset_type = 'COMPUTER';
                                if (grade && model && model_2 && info && info_2 && model?.toLowerCase().includes(model_2) && info?.toLowerCase().includes(info_2.toLowerCase())) {
                                    let fetchSOldQty = `select sum(quantity) tot_qty from public."Assets" where LOWER(processor) = '${itm2.processor ? itm2.processor.toLowerCase() : ''}' and LOWER(grade) = '${grade}' and LOWER(model) = '${model}' and status in ('RESERVATION','SOLD') and date_nor::date >  CURRENT_DATE - INTERVAL '2 months'`
                                    await database.raw(fetchSOldQty)
                                        .then(async (fetchSOldQtyResponse) => {
                                            if (fetchSOldQtyResponse.rows.length > 0 && fetchSOldQtyResponse.rows[0].tot_qty) {
                                                itm2.last_6months_grade = Math.round((Number(fetchSOldQtyResponse.rows[0].tot_qty) / itm.total_grade_sum) * 100)
                                                results.push(itm2)
                                                const result = await estimate_values_service.readByQuery({
                                                    fields: ["id"],
                                                    filter: {
                                                        asset_type: {
                                                            _eq: 'COMPUTER'
                                                        },
                                                        model: {
                                                            _icontains: model,
                                                        },
                                                        processor: {
                                                            _icontains: itm2.processor,
                                                        },
                                                        grade: {
                                                            _icontains: grade,
                                                        }
                                                    },
                                                });
                                                if (result.length > 0) {
                                                    return await estimate_values_service.updateOne(result[0].id,
                                                        itm2
                                                    ).then((response1) => {
                                                        // res.json(response);
                                                        console.log("update techvaluator", response1)
                                                    }).catch((error1) => {
                                                    });
                                                } else {
                                                    return await estimate_values_service.createOne(
                                                        itm2
                                                    ).then((response1) => {
                                                        // res.json(response);
                                                        // console.log("create techvaluator 1111", response1)
                                                    }).catch((error1) => {
                                                    });
                                                }
                                            } else {
                                                itm2.last_6months_grade = 0
                                                results.push(itm2)
                                                const result = await estimate_values_service.readByQuery({
                                                    fields: ["id"],
                                                    filter: {
                                                        asset_type: {
                                                            _eq: 'COMPUTER'
                                                        },
                                                        model: {
                                                            _icontains: model,
                                                        },
                                                        processor: {
                                                            _icontains: itm2.processor,
                                                        },
                                                        grade: {
                                                            _icontains: grade,
                                                        }
                                                    },
                                                });
                                                if (result.length > 0) {
                                                    return await estimate_values_service.updateOne(result[0].id,
                                                        itm2
                                                    ).then((response1) => {
                                                        // res.json(response);
                                                        console.log("update techvaluator", response1)
                                                    }).catch((error1) => {
                                                    });
                                                } else {
                                                    return await estimate_values_service.createOne(
                                                        itm2
                                                    ).then((response1) => {
                                                        // res.json(response);
                                                        // console.log("create techvaluator", response1)
                                                    }).catch((error1) => {
                                                    });
                                                }
                                            }
                                        })
                                }
                            }


                        }).catch((error1) => {
                            console.log("error ==> 1", error1)
                        });
                }
                if (!cond) {
                   return true
                }
            })
            .catch((error) => {

            });
    },
    UPDATEESTIMATEVALUEMOBILE: async (field, database, estimate_values_service, res) => {
        let cond = ''
        if (field) {
            cond = `and LOWER(hdd) = '${field.hdd ? field.hdd.toLowerCase() : ''}' and LOWER(model) = '${field.model ? field.model.toLowerCase() : ''}'`
        }
        let computerResponse = [];
        let results = [];
        let computerResponse1 = [];
        let sql4 = `SELECT 
		UPPER(hdd) hdd,
		UPPER(model) model,
		UPPER(form_factor) form_factor,
		string_agg(UPPER(hdd)::text, ',') as "mobile_info",
		SUM(quantity) AS total_grade_sum from
		public."Assets" where grade is not null 
		and UPPER(asset_type) in ('MOBILE','MOBILE DEVICE','MOBILE DEVICES') 
		and UPPER(grade) in ('A','B','C') 
		and LOWER(form_factor) in ('phone','tablet')
        and status in ('RESERVATION','SOLD')
		and date_nor::date >  CURRENT_DATE - INTERVAL '2 months'
        ${cond}
	    GROUP BY UPPER(model),UPPER(form_factor),UPPER(hdd)`
        // console.log("sql1", sql4)
        // and UPPER(model) like 'IPHONE 14 PRO BLACK'

        await database.raw(sql4)
            .then(async (CompuRes) => {
                computerResponse = CompuRes.rows;
                for (const itm of computerResponse) {
                    // return
                    // last 2 months total sold grade
                    let sql5 = `select UPPER(model) model,UPPER(hdd) hdd,UPPER(manufacturer) manufacturer,UPPER(form_factor) form_factor,
					string_agg(UPPER(hdd)::text, ',') as "mobile_info",
					UPPER(grade) grade,sum(quantity) sold_qty,
					sum(sold_price) as tot_soldprice 
					from public."Assets" 
					where UPPER(asset_type) in ('MOBILE','MOBILE DEVICE','MOBILE DEVICES') 
					and UPPER(grade)  in ('A','B','C') 
					and LOWER(form_factor) in ('phone','tablet') 
					and (date_nor is not null) 
					and date_nor::date >  CURRENT_DATE - INTERVAL '2 months' 
					and UPPER(hdd) like '${itm.hdd}' and UPPER(model) like '${itm.model}'
					and status in ('RESERVATION','SOLD')
					group by UPPER(model),UPPER(form_factor),UPPER(manufacturer),UPPER(hdd),UPPER(grade) order by UPPER(grade)`
                    // console.log("sql************", sql5)

                    await database.raw(sql5)
                        .then(async (CompuRes1) => {
                            computerResponse1 = CompuRes1.rows;
                            let form_factor = itm.form_factor.toLowerCase();
                            if (itm.mobile_info && (form_factor === 'phone' || form_factor === 'tablet')) {
                                itm.mobile_info_2 = _.uniq(itm.mobile_info.split(','));
                            }

                            for (const itm2 of computerResponse1) {
                                // console.log("itm2",itm2)
                                itm2.asset_type = 'MOBILE'
                                let avg_prvice = (itm2.tot_soldprice / itm2.sold_qty)
                                itm2.last_60_days_sold = avg_prvice ? Math.round(avg_prvice) : 0 // Grade values
                                itm2.last_60_days_qty_sold = itm2.sold_qty;
                                let form_factor_2 = itm2.form_factor.toLowerCase();
                                if (itm2.mobile_info && (form_factor_2 === 'phone' || form_factor_2 === 'tablet')) {
                                    itm2.mobile_info = _.uniq(itm2.mobile_info.split(','));
                                }
                                let hdd = itm.hdd?.toLowerCase().toString();
                                let hdd_2 = itm2.hdd?.toLowerCase().toString();
                                let grade = itm2.grade?.toLowerCase().toString();
                                let model = itm.model?.toLowerCase().toString();
                                let model_2 = itm2.model?.toLowerCase().toString();
                                let info = itm2.mobile_info?.toString();
                                let info_2 = itm.mobile_info_2?.toString();
                                if (grade && model && model_2 && info && info_2 && hdd && hdd_2 && (hdd_2 === hdd) && model?.toLowerCase().includes(model_2) && info?.toLowerCase().includes(info_2.toLowerCase())) {
                                    let fetchSOldQtyMobileSQL = `select sum(quantity) tot_qty from public."Assets" where LOWER(form_factor) = '${itm.form_factor ? itm.form_factor.toLowerCase() : ''}' and LOWER(grade) = '${grade}' and UPPER(hdd) like '${itm.hdd}' and LOWER(model) like '${model}' and status in ('RESERVATION','SOLD') and date_nor::date >  CURRENT_DATE - INTERVAL '2 months'`

                                    await database.raw(fetchSOldQtyMobileSQL)
                                        .then(async (fetchSOldQtyMobileResponse) => {
                                            //  console.log(fetchSOldQtyMobileSQL, "fetchSOldQtyMobileResponse.rows[0].tot_qty", fetchSOldQtyMobileResponse.rows[0].tot_qty)
                                            const estimate_values_service_data = await estimate_values_service.readByQuery({
                                                fields: ["id"],
                                                filter: {
                                                    asset_type: {
                                                        _eq: 'MOBILE'
                                                    },
                                                    model: {
                                                        _icontains: model,
                                                    },
                                                    hdd: {
                                                        _icontains: hdd,
                                                    },
                                                    grade: {
                                                        _icontains: grade,
                                                    }
                                                },
                                            });
                                            if (fetchSOldQtyMobileResponse.rows.length > 0 && fetchSOldQtyMobileResponse.rows[0].tot_qty) {
                                                itm2.last_6months_grade = Math.round((Number(fetchSOldQtyMobileResponse.rows[0].tot_qty) / Number(itm.total_grade_sum)) * 100)
                                                results.push(itm2)
                                                if (estimate_values_service_data.length > 0) {
                                                    return await estimate_values_service.updateOne(estimate_values_service_data[0].id,
                                                        itm2
                                                    ).then((response1) => {
                                                        // res.json(response);
                                                        // console.log("update techvaluator", response1)
                                                    }).catch((error1) => {
                                                    });
                                                } else {
                                                    return await estimate_values_service.createOne(
                                                        itm2
                                                    ).then((response1) => {
                                                        // res.json(response);
                                                        // console.log("create techvaluator 1111", response1)
                                                    }).catch((error1) => {
                                                    });
                                                }

                                            } else {
                                                itm2.last_6months_grade = 0
                                                results.push(itm2)
                                                if (estimate_values_service_data.length > 0) {
                                                    return await estimate_values_service.updateOne(estimate_values_service_data[0].id,
                                                        itm2
                                                    ).then((response1) => {
                                                        // res.json(response);
                                                        // console.log("update techvaluator", response1)
                                                    }).catch((error1) => {
                                                    });
                                                } else {
                                                    return await estimate_values_service.createOne(
                                                        itm2
                                                    ).then((response1) => {
                                                        // res.json(response);
                                                        // console.log("create techvaluator", response1)
                                                    }).catch((error1) => {
                                                    });
                                                }
                                            }
                                        }).catch((err_mob) => {
                                            console.log("error mobile ==> 1", err_mob)
                                        });
                                }
                            }


                        }).catch((error1) => {
                            console.log("error ==> 1", error1)
                        });
                }
                if (!cond) {
                    res.send({
                        data: [...results],
                        status: 200
                    })
                }
            })
            .catch((error) => {
                console.log("error ==> 1", error1)
            });
    },
    CREATEPROJECTIFNEWONE: async (input, projectService,ServiceUnavailableException) => {
        try {
            const projectdata = await projectService.readByQuery({
                fields: ["id", "project_type"],
                filter: {
                    id: {
                        _eq: input.project_id
                    }

                },
            });
            if (projectdata?.length === 0) {
                //New project created
                await projectService.createOne(
                    {
                        id: input.project_id,
                        warehouse: input.warehouse === 'NL01' ? 'NL01' : 'SE01'

                    })
            }

        } catch (e) {
            console.log("error", e)
            throw new ServiceUnavailableException(e);
        }
    },
    UPDATE_PART_NUMBER_ASSETS: async (id, partnumberService,assetsService, res)=> {
		try {
			const partnumber = await partnumberService.readByQuery({
				fields: ["action", "part_no", "status", "model", "asset_type", "form_factor", "manufacturer", 'co2', 'weight'],
				filter: {
					id: {
						_eq: id
					}

				},
			});

			if (partnumber?.length > 0) {
				let fields = partnumber[0];
				if (fields.status === 'published') {
					const assetList = await assetsService.readByQuery({
						fields: ["asset_id", "model", "asset_type", "form_factor", "manufacturer", "Part_No"],
						filter: {
							_or: [
								{ "Part_No": { _icontains: 'N/A' } },
								{ "Part_No": { _nnull: true } },
							],
							_and: [
								{ "Part_No": { _icontains: `${fields.part_no}` } }
							]
						},
						limit: -1
					});
                    const results = [];
                    const errors = [];
                    // Using for...of to handle async/await properly
                    let assetIds = assetList.map(
                        (item) => item.asset_id
                    );
                    for (const item of assetList || []) {
                      try {
                        // Example: Update or process item
                        // let sql = `update public."Assets" 
                        // set model = '${fields.model}', 
                        // asset_type = '${fields.asset_type}',
                        // manufacturer = '${fields.manufacturer}',
                        // form_factor = '${fields.form_factor}',
                        // sample_weight = '${fields.weight}',
                        // sample_co2 = '${fields.co2}'
                        // where asset_id = ${item.asset_id}`
                        // await database.raw(sql);
                        let obj = {
                            model: fields.model,
                            asset_type: fields.asset_type,
                            manufacturer: fields.manufacturer,
                            form_factor: fields.form_factor,
                            asset_id: item.asset_id,
                            sample_co2: fields.co2,
                            sample_weight: fields.weight,
                            part_number_update: 'true'
                        }
                        // const updated = await assetsService.updateOne(item.asset_id, obj);

                        const updated = await assetsService.updateMany(assetIds,
                            obj
                        )
                        results.push({ asset_id: item.asset_id, status: 'success', updated });
                      } catch (err) {
                        console.error(`Error updating item ${item.asset_id}:`, err);
                        errors.push({ asset_id: item.asset_id, status: 'failed', message: err.message });
                      }
                    }
              
                   // Return combined result after loop
                   if(res){
                    return res.json({
                      success: errors.length === 0,
                      processed: results.length,
                      failed: errors.length,
                      results,
                      errors,
                    });
                   }else {
                    return
                   }
                    
				}
			}
		} catch (error) {
            if(res){
                return res.json({
                success:false,
                msg:error
              });
            }
          
		}

	}
}

module.exports = mailbox;