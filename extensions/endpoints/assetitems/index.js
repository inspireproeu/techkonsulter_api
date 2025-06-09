const { UPDATEPROJECTFINANCE, NERDFIXBULKUPDATE, CREATENERDFIXHISTORY } = require('../../Functions');
const { ASSIGNSTOCKLISTVALUES, removeComplainData, removeComplainData_A_Grade, removeprocessorData, removeModelData } = require('../../Functions/mapvalues');

module.exports = async function registerEndpoint(router, app) {
	let _ = require('underscore')
	const ItemsService = app.services.ItemsService;
	const schema = await app.getSchema();
	// const accountabillity = await app.accountabillity();
	const database = app.database;
	const Excel = require('exceljs');
	const MailService = app.services.MailService;
	const mailService = new MailService({ schema });
	const projectService = new ItemsService('project', {
		schema
	});
	const assetsService = new ItemsService('Assets', {
		schema
	});
	const complaintsService = new ItemsService('complaints', {
		schema
	});
	const nerdfixservice = new ItemsService('nerdfixs_history', {
		schema
	});
	const estimate_values_service = new ItemsService('estimate_values_computer', {
		schema
	});

	let moment = require('moment')

	router.get("/pricingchart", async (req, res) => {
		let sql1 = `select to_char(date_nor1,'YY-MM') As datenor, sum(sold_price) as soldprice, sum(quantity) as tot_quantity from public."Assets" where UPPER(status)= 'SOLD' and sold_price !=0 and date_nor1 is NOT NULL group by datenor order by datenor desc`
		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				// console.log("resultttt", result)
				res.send({
					data: result,
					status: 200
				})
				// return {
				// 	data: result,
				// 	status: 200
				// }
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});
	router.get("/getmodels", async (req, res) => {
		let sql1 = `select UPPER(model) as asset_model, count(*) as cnt from public."Assets" where model is NOT NULL group by asset_model order by asset_model`

		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				res.send({
					data: result,
					status: 200
				})
				// return {
				// 	data: result,
				// 	status: 200
				// }
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});
	router.get("/getprocessors", async (req, res) => {
		let sql1 = `select UPPER(processor) as asset_processor, count(*) as cnt from public."Assets" where processor is NOT NULL group by asset_processor`

		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});
	router.get("/getgrades", async (req, res) => {
		let sql1 = `select UPPER(grade) as asset_grade, count(*) as cnt from public."Assets" where grade is NOT NULL group by asset_grade`

		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});
	router.get("/pricingassets", async (req, res) => {
		// let query = 
		// let sql1 = `select asset_id,grade,status,asset_type,manufacturer,model,processor,part_no,a_grade_values,default_grade,grade_last_update from public."Assets" where UPPER(asset_type) = '${req.query.asset_type}' ${req.query.manufacturer ? 'and UPPER(manufacturer) like' + req.query.manufacturer : '' } and UPPER(processor) like '${req.query.processor}' and UPPER(model) like '${req.query.model}' and UPPER(status) = 'IN STOCK'`
		let manufacturerQuery = `and manufacturer IS NULL`
		if (req.query.manufacturer) {
			manufacturerQuery = `and UPPER(manufacturer) like '${req.query.manufacturer}'`
		}
		let processorQuery = `and processor IS NULL`
		if (req.query.processor) {
			processorQuery = `and UPPER(processor) like '${req.query.processor}'`
		}
		let modelQuery = `and model IS NULL`
		if (req.query.model) {
			modelQuery = `and UPPER(model) like '${req.query.model}'`
		}
		let Part_NoQuery = `and "Part_No" IS NULL`
		if (req.query.Part_No) {
			Part_NoQuery = `and UPPER("Part_No") like '${req.query.Part_No}'`
		}
		// Part_No
		let sql1 = `select asset_id,grade,status,asset_type,manufacturer,model,processor,part_no,a_grade_values,default_grade,grade_last_update from public."Assets" where UPPER(asset_type) like '${req.query.asset_type}' ${processorQuery} ${modelQuery} ${Part_NoQuery} and UPPER(status) = 'IN STOCK'`
		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});
	router.get("/maintanceassets", async (req, res) => {
		let sql1 = `select sum(quantity) as qty,UPPER(status) as status,UPPER(asset_type) as asset_type,UPPER(manufacturer) as manufacturer,UPPER(model) as model,UPPER(processor) as processor,"Part_No",a_grade_values,default_grade,grade_last_update from public."Assets" where UPPER(status) = 'IN STOCK' group by UPPER(status),UPPER(asset_type),UPPER(manufacturer),UPPER(model),UPPER(processor),"Part_No",a_grade_values,default_grade,grade_last_update`
		// console.log("dataaaa", sql1)
		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});
	// router.get("/getProcessdate", async (req, res) => {
	// 	const projcets = await projectService.readByQuery({
	// 		fields: ["process_start_date", "id", "project_status", "finish_date"],
	// 		limit: -1,
	// 		filter: {
	// 			_and: [
	// 				{
	// 					no_of_assets_1: { _nnull: true }
	// 				},
	// 				{
	// 					project_status: { _in: ["ARRIVED"] }
	// 				}
	// 			]
	// 		}
	// 	});

	// 	// let result = response.rows;
	// 	// res.send({
	// 	// 	data: result,
	// 	// 	status: 200
	// 	// })
	// 	for (const data of projcets) {
	// 		// console.log("idddddddd", data.id)
	// 		const assetResult = await assetsService.readByQuery({
	// 			fields: ["asset_id", "status", "date_created"],
	// 			sort: ['-date_created'],
	// 			limit: -1,
	// 			filter: {
	// 				_and: [
	// 					{
	// 						project_id: { _eq: data.id }
	// 					}
	// 				]
	// 			}
	// 		});
	// 		console.log("***********", assetResult.length)

	// 		if (assetResult?.length > 0) {
	// 			console.log(data.id, "assetResult", assetResult.length);

	// 			//------------ send mail if first asset sold in the project
	// 			//---------end send mail if first asset sold in the project

	// 			//set project status as PROCESSING
	// 			let obj = {};
	// 			obj.project_sttaus = 'PROCESSING';
	// 			console.log("objjjjjjjjj", obj)
	// 			await projectService.updateOne(data.id,
	// 				obj
	// 			).then(async (response1) => {
	// 				console.log("Set project status as PROCESSING success", data.id);
	// 			}).catch((error1) => {
	// 				console.log("Set project status as PROCESSING error", data.id);
	// 			});



	// 		}
	// 	}
	// });

	router.get("/exportassets", async (req, res) => {
		let sql1 = `select sum(quantity) as count, asset_type, form_factor,"Part_No" as part_no, manufacturer, model, processor, memory, hdd, screen, UPPER(grade) as grade, target_price from public."Assets" where UPPER(status) like 'IN STOCK' and UPPER(asset_type) != 'ADAPTER' and UPPER(asset_type) != 'HDD' and (UPPER(asset_type) != 'COMPUTER' OR UPPER(asset_type) != 'MOBILE DEVICE' and (data_destruction = 'Erased' OR data_destruction = 'Powerwashed' OR data_destruction = 'Erased with warnings (Reallocated sectors erased' OR data_destruction ='No HDD')) and UPPER(grade) not in ('D','E','NORDIC') group by UPPER(model),UPPER(processor),UPPER(grade),hdd,UPPER(grade), asset_type,form_factor,"Part_No",manufacturer,model,processor,memory,screen,target_price`

		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				result.forEach((item) => {
					item.target_price = item.target_price ? Math.round(item.target_price) : ''
					removeModelData().forEach((removable) => {
						if (item.model) {
							item.model = item.model.toUpperCase().replace(removable, "").trim();
							return item.model;
						}
					})
					removeprocessorData().forEach((removable) => {
						if (item.processor) {
							item.processor = item.processor.toUpperCase().replace(removable, "").trim();
							return item.processor;
						}
					})
					removeComplainData().forEach((removable) => {
						if (item.complaint) {
							item.complaint = item.complaint.toUpperCase().replace(removable.toUpperCase(), "").trim();
							return item.complaint;
						}
					})

				})
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});


	//general stocklist
	router.get("/exportassetsnew", async (req, res) => {
		let cond = ''
		let warehouse = req.query.warehouse;
		let sql1 = `select DISTINCT asset_id,project_id,sum(quantity) as quantity,asset_id,asset_id_nl,imei,serial_number,storage_id,pallet_number,graphic_card,battery,keyboard, asset_type, form_factor,"Part_No" as part_no, manufacturer, model, processor, memory, hdd, screen, UPPER(grade) as grade, target_price,UPPER(complaint) complaint,complaint_from_app from public."Assets" ast INNER JOIN public.project prj on prj.id = ast.project_id and prj.warehouse = '${warehouse}' and UPPER(ast.status) like 'IN STOCK' and UPPER(asset_type) != 'ADAPTER' and UPPER(asset_type) != 'HDD' and UPPER(grade) in ('A','B','C','NOB','NEW','AB','A+') and ast.date_created < NOW() - INTERVAL '7 days' group by UPPER(model),UPPER(processor),UPPER(grade),hdd,UPPER(grade), asset_type,form_factor,"Part_No",manufacturer,model,processor,memory,screen,target_price,UPPER(complaint),project_id,asset_id,imei,serial_number,storage_id,pallet_number,graphic_card,battery,keyboard,quantity`
		if (req.query.action) {
			let type = req.query.type;
			if (type === 'computerparts') {
				cond = `UPPER(asset_type) = 'PARTS COMPUTER' and `
			} else if (type === 'serverstorage') {
				cond = `UPPER(asset_type) = 'SERVER & STORAGE' and `
			} else if (type === 'serverparts') {
				cond = `UPPER(asset_type) = 'PARTS SERVER' and `
			} else if (type === 'network') {
				cond = `UPPER(asset_type) = 'NETWORK' and `
			} else if (type === 'printer') {
				cond = `UPPER(asset_type) = 'PRINTER' and `
			} else if (type === 'pos') {
				cond = `UPPER(asset_type) = 'POS' and `
			}
			sql1 = `select DISTINCT asset_id,project_id,sum(quantity) as quantity,asset_id,asset_id_nl,imei,serial_number,storage_id,pallet_number,graphic_card,battery,keyboard, asset_type, form_factor,"Part_No" as part_no, manufacturer, model, processor, memory, hdd, screen, UPPER(grade) as grade, target_price,UPPER(data_destruction) data_destruction,UPPER(complaint) complaint,complaint_from_app from public."Assets" ast INNER JOIN public.project prj on prj.id = ast.project_id and prj.warehouse = '${warehouse}' and ${cond} UPPER(ast.status) like 'IN STOCK' and UPPER(asset_type) != 'ADAPTER' and UPPER(asset_type) != 'HDD' and UPPER(grade) in ('A','B','C','NOB','NEW','AB','A+') and ast.date_created < NOW() - INTERVAL '7 days' group by UPPER(model),UPPER(processor),UPPER(grade),hdd,UPPER(grade), asset_type,form_factor,"Part_No",manufacturer,model,processor,memory,screen,target_price,UPPER(data_destruction),UPPER(complaint),project_id,asset_id,imei,serial_number,storage_id,pallet_number,graphic_card,battery,keyboard,quantity`
		} else if (!req.query.action) {
			//This is for stocklist
			sql1 = `select DISTINCT asset_id,project_id,sum(quantity) as quantity,asset_id,asset_id_nl,imei,serial_number,storage_id,pallet_number,graphic_card,battery,keyboard, asset_type, form_factor,"Part_No" as part_no, manufacturer, model, processor, memory, hdd, screen, UPPER(grade) as grade, target_price,UPPER(data_destruction) data_destruction,UPPER(complaint) complaint,complaint_from_app from public."Assets" ast INNER JOIN public.project prj on prj.id = ast.project_id and prj.warehouse = '${warehouse}' and UPPER(ast.status) like 'IN STOCK' and UPPER(asset_type) != 'ADAPTER' and UPPER(asset_type) != 'HDD' and UPPER(grade) in ('A','B','C','NOB','NEW','AB','A+') group by UPPER(model),UPPER(processor),UPPER(grade),hdd,UPPER(grade), asset_type,form_factor,"Part_No",manufacturer,model,processor,memory,screen,target_price,UPPER(data_destruction),UPPER(complaint),project_id,asset_id,imei,serial_number,storage_id,pallet_number,graphic_card,battery,keyboard,quantity`
		}

		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				result = await ASSIGNSTOCKLISTVALUES(result, 'general');
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});

	//computer stocklist
	router.get("/exportassetscomputer", async (req, res) => {
		let warehouse = req.query.warehouse;
		let sql1 = `select DISTINCT asset_id,sum(quantity) as quantity, project_id,asset_id,asset_id_nl,serial_number,UPPER(model) as model,UPPER(processor) as processor,UPPER(grade) as grade,hdd, asset_type,form_factor,"Part_No",graphic_card,hdd,manufacturer,battery, complaint,keyboard,model,pallet_number,storage_id,processor,memory,screen,target_price,complaint_from_app,UPPER(data_destruction) data_destruction from public."Assets" ast INNER JOIN public.project prj on prj.id = ast.project_id and prj.warehouse = '${warehouse}' and UPPER(ast.status) like 'IN STOCK' and UPPER(asset_type) = 'COMPUTER' and UPPER(grade) in ('A', 'B', 'C', 'NOB','NEW','AB','A+') and (complaint is null or complaint = '' or complaint not like '%HDD from%' OR complaint not like '%HDDs from%') and ast.date_created < NOW() - INTERVAL '7 days' group by project_id,asset_id,serial_number,UPPER(model),UPPER(processor),UPPER(grade),hdd, asset_type,form_factor,"Part_No",graphic_card,hdd,manufacturer,battery, complaint,keyboard,model,pallet_number,storage_id,processor,memory,screen,target_price,UPPER(data_destruction)`
		if (!req.query.action) {
			//This is for stocklist
			sql1 = `select project_id,asset_id,asset_id_nl,asset_type,form_factor,manufacturer,UPPER(model) as model,serial_number,UPPER(processor) as processor,memory,hdd,graphic_card,battery,keyboard,UPPER(grade) as grade,complaint,target_price,pallet_number,storage_id,complaint_from_app,UPPER(data_destruction) data_destruction from public."Assets" ast INNER JOIN public.project prj on prj.id = ast.project_id and prj.warehouse = '${warehouse}' and UPPER(ast.status) like 'IN STOCK' and UPPER(asset_type) = 'COMPUTER' and UPPER(grade) in ('A', 'B', 'C', 'NOB','NEW','AB','A+') and (complaint is null or complaint = '' or complaint not like '%HDD from%' OR complaint not like '%HDDs from%') group by project_id,asset_id,serial_number,UPPER(model),UPPER(processor),UPPER(grade),hdd, asset_type,form_factor,"Part_No",graphic_card,hdd,manufacturer,battery, complaint,keyboard,model,pallet_number,storage_id,processor,memory,screen,target_price,UPPER(data_destruction)`
		}
		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				result = await ASSIGNSTOCKLISTVALUES(result, null, 'computer');
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});

	//Download Mobile device stocklists

	router.get("/exportassetsmobile", async (req, res) => {
		let warehouse = req.query.warehouse;
		let sql1 = `select asset_id,asset_id_nl,project_id,asset_type,form_factor,manufacturer,model,imei,serial_number,hdd,battery,grade,complaint,target_price,pallet_number,storage_id,complaint_from_app from public."Assets" ast INNER JOIN public.project prj on prj.id = ast.project_id and prj.warehouse = '${warehouse}' and LOWER(TRIM(asset_type)) like 'mobile%' and UPPER(ast.status) = 'IN STOCK' and LOWER(data_destruction) = 'erased' and UPPER(grade) in ('A', 'B', 'C', 'NOB','NEW','AB','A+') and ast.date_created < NOW() - INTERVAL '7 days' order by ast.date_created desc`
		if (!req.query.action) {
			sql1 = `select project_id,asset_id,asset_id_nl,asset_type,form_factor,manufacturer,model,imei,serial_number,hdd,battery,grade,complaint,target_price,pallet_number,storage_id,complaint_from_app from public."Assets"  ast INNER JOIN public.project prj on prj.id = ast.project_id and prj.warehouse = '${warehouse}' and LOWER(TRIM(asset_type)) like 'mobile%' and UPPER(ast.status) = 'IN STOCK' and LOWER(data_destruction) = 'erased' and UPPER(grade) in ('A', 'B', 'C', 'NOB','NEW','AB','A+') order by ast.date_created desc`
		}
		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				result.forEach((item) => {
					item.target_price = item.target_price ? parseFloat(Math.round(item.target_price)) : 0
					removeComplainData().forEach((removable) => {
						if (item.complaint) {
							item.complaint = item.complaint.toUpperCase().replace(removable.toUpperCase(), "").trim();
							return item.complaint;
						}
					})
					removeComplainData_A_Grade().forEach((removable) => {
						if (item.grade === "A" && item.complaint) {
							item.complaint = item.complaint.toUpperCase().replace(removable.toUpperCase(), "").trim();
							return item.complaint;
						}
					})
					if (item?.complaint_from_app && !item?.complaint) {
						item.complaint = item?.complaint_from_app
					}
				})

				// res.send({
				// 	data: datavalues,
				// 	status: 200
				// })

				let sql11 = `select asset_id,asset_id_nl,project_id,asset_type,form_factor,manufacturer,model,imei,serial_number,hdd,battery,grade,complaint,target_price,pallet_number,storage_id,complaint_from_app from public."Assets"  ast INNER JOIN public.project prj on prj.id = ast.project_id and prj.warehouse = '${warehouse}' and LOWER(TRIM(asset_type)) like 'mobile%' and UPPER(ast.status) = 'IN STOCK' and data_destruction is null and UPPER(grade) in ('NOB','NEW') and ast.date_created < NOW() - INTERVAL '7 days' order by ast.date_created desc`
				if (!req.query.action) {
					sql11 = `select project_id,asset_id,asset_id_nl,asset_type,form_factor,manufacturer,model,imei,serial_number,hdd,battery,grade,complaint,target_price,pallet_number,storage_id,complaint_from_app from public."Assets"  ast INNER JOIN public.project prj on prj.id = ast.project_id and prj.warehouse = '${warehouse}' and LOWER(TRIM(asset_type)) like 'mobile%' and UPPER(ast.status) = 'IN STOCK' and data_destruction is null and UPPER(grade) in ( 'NOB','NEW') order by ast.date_created desc`
				}
				await database.raw(sql11)
					.then(async (response) => {
						let result2 = response.rows;
						result2.forEach((item) => {
							item.target_price = item.target_price ? parseFloat(Math.round(item.target_price)) : 0
							removeComplainData().forEach((removable) => {
								if (item.complaint) {
									item.complaint = item.complaint.toUpperCase().replace(removable.toUpperCase(), "").trim();
									return item.complaint;
								}
							})
							removeComplainData_A_Grade().forEach((removable) => {
								if (item.grade === "A" && item.complaint) {
									item.complaint = item.complaint.toUpperCase().replace(removable.toUpperCase(), "").trim();
									return item.complaint;
								}
							})
							if (item?.complaint_from_app && !item?.complaint) {
								item.complaint = item?.complaint_from_app
							}
						})

						res.send({
							data: [...result, ...result2],
							status: 200
						})
					})
					.catch((error) => {
						res.send({
							data: error,
							status: 500
						})
					});

			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});

	router.get("/exportassetsValues", async (req, res) => {
		// let sql1_old = `select asset_type as Type,UPPER(manufacturer) as manufacturer,UPPER(model) as model,UPPER(processor) as CPU,"Part_No" as "Part number", MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice  from public."Assets" where date_nor != '' and date_nor::date >  CURRENT_DATE - INTERVAL '3 months' and UPPER(status) NOT like 'IN STOCK' and UPPER(status) NOT like 'RECYCLED' and (UPPER(status) like 'SOLD' or UPPER(status) like 'RESERVATION') and (grade = 'A' or grade = 'B') group by asset_type,"Part_No",UPPER(manufacturer),UPPER(model),UPPER(processor)`
		let sql1 = `select asset_type as Type,UPPER(manufacturer) as manufacturer,UPPER(model) as model,UPPER(processor) as CPU,"Part_No" as "Part number", MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice  from public."Assets" where (UPPER(asset_type) = 'COMPUTER' or UPPER(asset_type) = 'MOBILE DEVICE' OR (UPPER(asset_type) = 'NETWORK' and UPPER(form_factor) = 'SWITCH') OR (UPPER(asset_type) = 'SERVER & STORAGE' and UPPER(form_factor) = 'RACK MOUNT CHASSIS') OR (UPPER(asset_type) = 'SERVER & STORAGE' and UPPER(form_factor) = 'BLADE SERVER')) and  date_nor != '' and date_nor is not null and date_nor != 'Invalid date' and date_nor::date >  CURRENT_DATE - INTERVAL '3 months' and UPPER(status) NOT like 'IN STOCK' and UPPER(status) NOT like 'RECYCLED' and (UPPER(status) like 'SOLD' or UPPER(status) like 'RESERVATION') and (UPPER(grade) = 'A' or UPPER(grade) = 'B') group by asset_type,"Part_No",UPPER(manufacturer),UPPER(model),UPPER(processor) order by model`

		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				result.forEach((item) => {
					let min = Math.round(item.min_sales - (item.min_sales / 100) * 10)
					let max = Math.round(item.max_sales + (item.max_sales / 100) * 10)
					let avg_prvice = (item.tot_soldprice / item.sold_qty)
					item["Working unit from/to value €"] = (min + "-" + max)
					item["Working unit average value €"] = avg_prvice ? Math.round(avg_prvice) : 0
				})
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});

	router.get("/brokerbin", async (req, res) => {
		// let sql1_old = `select asset_type as Type,UPPER(manufacturer) as manufacturer,UPPER(model) as model,UPPER(processor) as CPU,"Part_No" as "Part number", MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice  from public."Assets" where date_nor != '' and date_nor::date >  CURRENT_DATE - INTERVAL '3 months' and UPPER(status) NOT like 'IN STOCK' and UPPER(status) NOT like 'RECYCLED' and (UPPER(status) like 'SOLD' or UPPER(status) like 'RESERVATION') and (grade = 'A' or grade = 'B') group by asset_type,"Part_No",UPPER(manufacturer),UPPER(model),UPPER(processor)`
		//let sql1 = `select  "Part_No" as "Part Number",UPPER(grade) as "Condition",model as "Description",manufacturer as "Manufacturer",target_price as "Price",sum(quantity) as "Quantity"  from public."Assets" where "Part_No" is not null and "Part_No" != '' and (UPPER(asset_type) = 'NETWORK' OR UPPER(asset_type) = 'SERVER & STORAGE' OR UPPER(asset_type) = 'DOCKING') and LOWER(status) = 'in stock' and (UPPER(grade) = 'A' or UPPER(grade) = 'NEW' OR UPPER(grade) = 'NOB') group by UPPER(grade), "Part_No",manufacturer,target_price,model`
		let sql1 = `select  "Part_No" as "Part Number",UPPER(grade) as "Condition",model as "Description",manufacturer as "Manufacturer",target_price as "Price",sum(quantity) as "Quantity"  from public."Assets" where "Part_No" is not null and "Part_No" != '' and UPPER(asset_type) in ('NETWORK', 'SERVER & STORAGE', 'DOCKING','POS','COMPONENTS','CABLE','MISC','TELECOM & VIDEOCONFERENCE') and LOWER(status) = 'in stock' and (UPPER(grade) = 'A' or UPPER(grade) = 'NEW' OR UPPER(grade) = 'NOB') group by UPPER(grade), "Part_No",manufacturer,target_price,model`

		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				result.forEach((item) => {
					if (item.Condition === 'USED') {
						item.Condition = 'REF';
					}
					else if (item.Condition === 'A') {
						item.Condition = 'USED';
					}
					item.Price = 'CALL';
					if (item.Description) {
						item.Description = "ITAD SOURCE - " + item.Description;
					}
				})
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});
	router.post("/certusmobile", async (req, res) => {
		const itemsService = new ItemsService('CertusMobile', {
			schema
		});
		const assetsService = new ItemsService('Assets', {
			schema
		});
		let assetsData = (req.body);
		// console.log("assetsData.length",assetsData.length)
		for (let i = 0; i <= assetsData.length; i++) {
			if (assetsData[i] && assetsData[i].asset_id) {
				let asset_id = assetsData[i].asset_id
				let sql = `select * from public."CertusMobile" where asset_id = '${asset_id}'`
				await database.raw(sql)
					.then(async (results) => {
						let resMobile = results.rows;
						if (resMobile.length === 0) {
							// console.log(getCertusMapped(assetsData[i]), "asetssss length 222", res.length)
							// certusData.push(mapCertusData(assetsData[i]));
							// certusData = _.sortBy(certusData, function (o) { return new Date(o.erasure_ended).getTime() * -1; })
							const activity = await itemsService.createOne(
								assetsData[i]
							).then(async (response1) => {
								// res.json(response);
								// console.log("create certus", response1.length)
								const activity1 = await assetsService.createOne(
									getCertusMapped(assetsData[i])
								).then((response2) => {
									res.status('200').send({ message: 'create successful asset mobile', status: 'success' });

								}).catch((error1) => {
									console.log("certus assets create error", error1)

								});
								console.log("activity1", activity1)
							}).catch((error1) => {
								console.log("certus mobile create error", error1)
							});
						} else {
							const activity = await itemsService.updateOne(
								assetsData[i].asset_id,
								assetsData[i]
							).then(async (response1) => {
								console.log("update certus", response1)
								const activity1 = await assetsService.updateOne(
									assetsData[i].asset_id, getCertusMapped(assetsData[i])
								).then((response2) => {
									// res.json(response);
									res.status('200').send({ message: 'update successful asset mobile', status: 'success' });
									console.log("update certus assets", response2)
								}).catch((error1) => {
									console.log("certus assets create error", error1)
								});
							}).catch((error1) => {
								console.log("certus mobile update error", error1)

							});
						}
					})
					.catch((error) => {
						console.log("errrrr", error)
					});

			}
		}
		// let sql1 = `select project_id, asset_id, asset_type, form_factor, Part_No, quantity, manufacturer, model, imei, serial_number, processor, memory, hdd, optical, graphic_card, battery, keyboard, screen, description, grade, complaint, target_price from public."Assets" where UPPER(status) like 'IN STOCK' and UPPER(asset_type) != 'ADAPTER' and (data_destruction = 'Erased' OR data_destruction = 'Powerwashed' OR data_destruction = 'Erased with warnings (Reallocated sectors erased' OR data_destruction ='No HDD')`

		// await database.raw(sql1)
		// 	.then(async (response) => {
		// 		let result = response.rows;
		// 		res.send({
		// 			data: result,
		// 			status: 200
		// 		})
		// 	})
		// 	.catch((error) => {
		// 		res.send({
		// 			data: [],
		// 			status: 500
		// 		})
		// 	});
	});


	// router.post("/getTargetPrice", async (req, res) => {


	// 	let assetsData = (req.body);
	// 	await callTargetPrice(assetsData, true, res);
	// });
	// async function callTargetPrice(field = null, isSingle = false, res) {
	// 	let cond = ''
	// 	let cond2 = ''
	// 	if (isSingle) {
	// 		let model = '';
	// 		let processor = '';
	// 		let grade = '';
	// 		let memory = '';
	// 		let Part_No = '';
	// 		// -------------------------------------------------------------fetching in mobile data

	// 		if (field.processor) {
	// 			removeprocessorData().forEach((removable) => {
	// 				field.processor = field.processor.toUpperCase().replace(removable.toUpperCase(), "").trim();
	// 			})
	// 			field.processor = field.processor ? field.processor.toUpperCase() : ''
	// 		}
	// 		if (field.model) {
	// 			removeModelData().forEach((removable) => {
	// 				field.model = field.model.toUpperCase().replace(removable.toUpperCase(), "").trim();
	// 			})
	// 		}
	// 		if (field.model) {
	// 			model = `model like '${field.model}'`
	// 		} else {
	// 			model = `(model is null OR model ='')`
	// 		}
	// 		if (field.processor) {
	// 			processor = `processor like '${field.processor}'`
	// 		} else {
	// 			processor = `(processor is null OR processor = '')`
	// 		}
	// 		if (field.grade) {
	// 			grade = `grade = '${field.grade}'`
	// 		} else {
	// 			grade = `(grade is null OR grade = '')`
	// 		}
	// 		cond = `and ${model} and ${processor} and ${grade}`;
	// 		// -------------------------------------------------------------fetching not in mobile data
	// 		if (field.memory) {
	// 			memory = `memory = '${field.memory}'`
	// 		} else {
	// 			memory = `(memory is null OR memory = '')`
	// 		}
	// 		if (field.Part_No) {
	// 			Part_No = `Part_No = '${field.Part_No}'`
	// 		} else {
	// 			Part_No = `(Part_No is null OR Part_No = '')`
	// 		}
	// 		cond2 = `and ${model} and ${processor} and ${grade} and ${memory} and ${Part_No}`;
	// 	}
	// 	// Fetch 'MONITOR' ,'COMPUTER' ,'MOBILE DEVICE' values
	// 	let sql11 = `select UPPER(asset_type) as asset_type,UPPER(model) as model,UPPER(processor) as processor, UPPER(grade) as grade, MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice from public."Assets" where UPPER(asset_type)  in ('MONITOR' ,'COMPUTER' ,'MOBILE DEVICE') and UPPER(status) in ('SOLD' ,'RESERVATION') and (date_nor != '' and date_nor is not null and date_nor != 'Invalid date') and date_nor::date >  CURRENT_DATE - INTERVAL '2 months' ${cond} group by UPPER(asset_type),UPPER(grade),UPPER(model),UPPER(processor)`
	// 	//let sql1 = `select UPPER(asset_type) as asset_type,UPPER(model) as model,UPPER(processor) as processor, UPPER(grade) as grade, MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice from public."Assets" where (UPPER(asset_type) = 'MONITOR' OR UPPER(asset_type) = 'COMPUTER' or UPPER(asset_type) = 'MOBILE DEVICE') and (UPPER(status) like 'SOLD' OR UPPER(status) like 'RESERVATION') and  date_nor != '' and date_nor::date >  CURRENT_DATE - INTERVAL '6 months' group by UPPER(asset_type),UPPER(grade),UPPER(model),UPPER(processor)`

	// 	await database.raw(sql11)
	// 		.then(async (response) => {
	// 			let result = response.rows;
	// 			if (result?.length > 0) {
	// 				result.forEach((item) => {
	// 					let avg_prvice = (item.tot_soldprice / item.sold_qty)
	// 					let percentage = Math.round((avg_prvice / 100) * 10)
	// 					let target_price = avg_prvice + percentage
	// 					item["target_price"] = Math.round(target_price)
	// 				})
	// 				res.send({
	// 					data: result[0]?.target_price || 0
	// 				})
	// 				return
	// 			} else {
	// 				res.send({
	// 					data: 0
	// 				})
	// 				// res.status(200).json({ data: 0 });

	// 			}
	// 		})
	// 		.catch((error) => {
	// 			res.send({
	// 				data: 0
	// 			})
	// 		});

	// 	// Fetch not in 'MONITOR' ,'COMPUTER' ,'MOBILE DEVICE' values
	// 	let sql2 = `select "Part_No", memory,UPPER(asset_type) as asset_type,UPPER(model) as model,UPPER(processor) as processor, UPPER(grade) as grade, MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice from public."Assets" where UPPER(asset_type) not in ('MONITOR' ,'COMPUTER' ,'MOBILE DEVICE') and UPPER(status) in ('SOLD' ,'RESERVATION') and (date_nor != '' and date_nor is not null and date_nor != 'Invalid date') and date_nor::date >  CURRENT_DATE - INTERVAL '6 months' ${cond2} group by UPPER(asset_type),UPPER(grade),UPPER(model),UPPER(processor),"Part_No",memory`
	// 	await database.raw(sql2)
	// 		.then(async (response) => {
	// 			let result = response.rows;
	// 			if (result?.length > 0) {
	// 				result.forEach((item) => {
	// 					let avg_prvice = (item.tot_soldprice / item.sold_qty)
	// 					let percentage = Math.round((avg_prvice / 100) * 20)
	// 					let target_price = avg_prvice + percentage
	// 					item["target_price"] = Math.round(target_price)
	// 				})
	// 				res.send({
	// 					data: result[0]?.target_price || 0
	// 				})
	// 				// res.status(200).send({
	// 				// 	data: result[0]?.target_price || 0
	// 				// })
	// 				// res.status(200).json({ data: result[0]?.target_price || 0 });
	// 				return
	// 			} else {
	// 				res.send({
	// 					data: 0
	// 				})
	// 				// res.status(200).json({ data: 0 });

	// 			}
	// 		})
	// 		.catch((error) => {
	// 			res.send({
	// 				data: 0
	// 			})

	// 		});
	// }

	function getCertusMapped(data) {
		let status = '';
		let grade = '';
		if (data.grade) {
			grade = data.grade.toLowerCase()
		}
		if (grade === 'a' || grade === 'b' || grade === 'c' || grade === 'd') {
			status = 'IN STOCK';
		} else if (data.grade === 'e') {
			status = "Recycled";
		} else {
			status = 'IN STOCK';
		}
		// let assettype = data.chasis_type.toLowerCase();
		// if (assettype === 'mobile device') {
		//   assettype = 'mobile';
		// }
		const assetdata = {
			project_id: data.project_id || '',
			manufacturer: data.system_manufacturer || '',
			serial_number: data.serial_number || '',
			data_destruction: data.erasure_status || '',
			grade: grade || '',
			status: status,
			quantity: data.quantity || '1',
			asset_id: data.asset_id.toString().replace(/[\W_]/g, "") || '',
			asset_type: 'Mobile device',
			model: data.device_model || '',
			form_factor: data.report_operator || '',
			hdd: data.hdd || '',
			memory: data.device_internal_memory || '',
			coa: data.device_operating_system || '',
			battery: data.device_battery_health || '',
			complaint: data.comments || '',
			description: data.device_model_number || '',
			"Part_No": data.device_model_number || '',
			imei: data.device_imei || '',
			erasure_ended: data.erasure_ended || '',
			data_generated: 'CERTUSMOBILE'
		}
		return assetdata
	}
	//Mobile market price techkonsulter external page
	router.get("/mobilemarketprice", async (req, res) => {
		let manufacturerQuery = ''
		if (req.query.manufacturer) {
			manufacturerQuery = `and UPPER(manufacturer) like '${req.query.manufacturer.toUpperCase()}'`
		}

		let sql1 = `select UPPER(manufacturer) manufacturer, UPPER(model) model,count(*) sold_qty,sum(sold_price) tot_soldprice from public."Assets" where manufacturer !='' and model !='' ${manufacturerQuery} and UPPER(grade) in ('A','B') and date_nor != 'Invalid date' and date_nor != '' and date_nor is not null and  date_nor::date >  CURRENT_DATE - INTERVAL '60 day'  group by UPPER(manufacturer),UPPER(model)`

		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				result.forEach((item) => {
					let avg_prvice = (item.tot_soldprice / item.sold_qty)
					item.last_60_days_sold = avg_prvice ? Math.round(avg_prvice) : 0
				})
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});


	//manufaturers
	router.get("/manufacturers", async (req, res) => {

		let sql1 = `select UPPER(manufacturer) manufacturer from public."Assets" where manufacturer !=''and manufacturer is not null group by UPPER(manufacturer) order by manufacturer`

		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});

	router.get("/estimateassetvalues_computer", async (req, res) => {
		// Estimate value A- Grade - COMPUTER TYPE
		let computerResponse = [];
		let computerResponse1 = [];
		let results = [];
		let sql1 = `SELECT
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
		GROUP BY UPPER(model),UPPER(processor),UPPER(form_factor) limit 500`;
		 console.log("sql1",sql1)
		await database.raw(sql1)
			.then(async (CompuRes) => {
				console.log("===========>", CompuRes.rows)
				computerResponse = CompuRes.rows;
				for (const itm of computerResponse) {
					// return
					// last 2 months total sold grade
					let sql2 = `select UPPER(model) model,UPPER(processor) processor,UPPER(form_factor) form_factor,
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
				group by UPPER(model),UPPER(form_factor),UPPER(processor),UPPER(grade) order by UPPER(grade)`
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
											results.push(itm2)
											const estimate_values_service_data = await estimate_values_service.readByQuery({
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
											if (fetchSOldQtyResponse.rows.length > 0 && fetchSOldQtyResponse.rows[0].tot_qty) {
												itm2.last_6months_grade = Math.round((Number(fetchSOldQtyResponse.rows[0].tot_qty) / itm.total_grade_sum) * 100)
												if (estimate_values_service_data.length > 0) {
													return await estimate_values_service.updateOne(estimate_values_service_data[0].id,
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
														console.log("create techvaluator 1111", response1)
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
														console.log("update techvaluator", response1)
													}).catch((error1) => {
													});
												} else {
													return await estimate_values_service.createOne(
														itm2
													).then((response1) => {
														// res.json(response);
														console.log("create techvaluator", response1)
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

				res.send({
					data: [...results],
					status: 200
				})

			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});


	router.get("/estimateassetvalues_mobile", async (req, res) => {
		// Estimate value A- Grade - COMPUTER TYPE
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
		and date_nor::date >  CURRENT_DATE - INTERVAL '2 months'
	GROUP BY UPPER(model),UPPER(form_factor),UPPER(hdd)`
		// console.log("sql1", sql4)
		// and UPPER(model) like 'IPHONE 14 PRO BLACK'

		await database.raw(sql4)
			.then(async (CompuRes) => {
				computerResponse = CompuRes.rows;
				console.log("computerResponse", computerResponse)
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
														console.log("update techvaluator", response1)
													}).catch((error1) => {
													});
												} else {
													return await estimate_values_service.createOne(
														itm2
													).then((response1) => {
														// res.json(response);
														console.log("create techvaluator 1111", response1)
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
														console.log("update techvaluator", response1)
													}).catch((error1) => {
													});
												} else {
													return await estimate_values_service.createOne(
														itm2
													).then((response1) => {
														// res.json(response);
														console.log("create techvaluator", response1)
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

				res.send({
					data: [...results],
					status: 200
				})

			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});


	//Estimated asset values
	router.get("/estimateassetvalues_mobile_bkk", async (req, res) => {

		// Estimate value A- Grade - COMPUTER TYPE
		let mobileResponse2 = [];
		let mobile6monthsresponse = [];
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
		and date_created::date >  CURRENT_DATE - INTERVAL '2 months'
	GROUP BY UPPER(model),UPPER(form_factor),UPPER(hdd)`
		await database.raw(sql4)
			.then(async (mobileRes2) => {
				mobileResponse2 = mobileRes2.rows;
				if (mobileResponse2?.length > 0) {
					// last 2 months total sold gradeand and UPPER(hdd) like '${itm.hdd}' and LOWER(model) like '${model}' UPPER(hdd) like '${itm.hdd}' and LOWER(model) like '${model}'and UPPER(hdd) like '${itm.hdd}' and LOWER(model) like '${model}'ER(hdd) like '${itm.hdd}' and LOWER(model) like '${model}'
					let sql5 = `select UPPER(model) model,UPPER(hdd) hdd,UPPER(manufacturer) manufacturer,UPPER(form_factor) form_factor,string_agg(UPPER(hdd)::text, ',') as "mobile_info",UPPER(grade) grade,sum(quantity) sold_qty,sum(sold_price) as tot_soldprice 
					from public."Assets" 
					where UPPER(asset_type) in ('MOBILE','MOBILE DEVICE','MOBILE DEVICES') 
					and UPPER(grade)  in ('A','B','C') 
					and LOWER(form_factor) in ('phone','tablet') 
					and (date_nor is not null) 
					and date_nor::date >  CURRENT_DATE - INTERVAL '2 months' 
					and UPPER(hdd) like '${itm.hdd}' and LOWER(model) like '${model}'
					group by UPPER(model),UPPER(form_factor),UPPER(manufacturer),UPPER(hdd),UPPER(grade) order by UPPER(grade)`
					// let sql2 = `select grade,100.0 * quantity / SUM(quantity) OVER (PARTITION BY grade) AS percent from public."Assets" where UPPER(model)='ELITEBOOK 830 G6' and UPPER(processor)='I5-8265U' and UPPER(asset_type) in ('COMPUTER') and UPPER(grade) in ('A','B','C') and LOWER(form_factor) in ('desktop','laptop') and date_created::date >  CURRENT_DATE - INTERVAL '6 months' group by grade`
					await database.raw(sql5)
						.then(async (mobileres) => {
							mobile6monthsresponse = mobileres.rows;
							for (const itm of mobileResponse2) {
								let form_factor = itm.form_factor.toLowerCase();
								if (itm.mobile_info && (form_factor === 'phone' || form_factor === 'tablet')) {
									itm.mobile_info = _.uniq(itm.mobile_info.split(','));
								}
								let avg_prvice = (itm.tot_soldprice / itm.sold_qty)
								itm.last_60_days_sold = avg_prvice ? Math.round(avg_prvice) : 0 // Grade values
								itm.last_60_days_qty_sold = itm.sold_qty;
								for (const itm2 of mobile6monthsresponse) {
									let form_factor_2 = itm2.form_factor.toLowerCase();
									if (itm2.mobile_info && (form_factor_2 === 'phone' || form_factor_2 === 'tablet')) {
										itm2.mobile_info_2 = _.uniq(itm2.mobile_info.split(','));
									}
									let hdd = itm.hdd?.toLowerCase().toString();
									let hdd_2 = itm2.hdd?.toLowerCase().toString();
									let grade = itm.grade?.toLowerCase().toString();
									let model = itm.model?.toLowerCase().toString();
									let model_2 = itm2.model?.toLowerCase().toString();
									let info = itm.mobile_info?.toString();
									let info_2 = itm2.mobile_info_2?.toString();
									if (grade && model && model_2 && info && info_2 && hdd && hdd_2 && (hdd_2 === hdd) && model?.toLowerCase().includes(model_2) && info?.toLowerCase().includes(info_2.toLowerCase())) {
										let fetchSOldQtyMobileSQL = `select sum(quantity) tot_qty from public."Assets" where LOWER(form_factor) = '${itm.form_factor ? itm.form_factor.toLowerCase() : ''}' and UPPER(manufacturer) like '${itm.manufacturer}' and LOWER(grade) = '${grade}' and UPPER(hdd) like '${itm.hdd}' and LOWER(model) like '${model}' and status in ('RESERVATION') and date_created::date >  CURRENT_DATE - INTERVAL '2 months'`
										await database.raw(fetchSOldQtyMobileSQL)
											.then(async (fetchSOldQtyMobileResponse) => {
												if (fetchSOldQtyMobileResponse.rows.length > 0 && fetchSOldQtyMobileResponse.rows[0].tot_qty) {
													itm.last_6months_grade = Math.round((Number(fetchSOldQtyMobileResponse.rows[0].tot_qty) / Number(itm2.total_grade_sum)) * 100)
												}
											}).catch((err_mob) => {
												console.log("error mobile ==> 1", err_mob)
											});
									}
								}
							}
							res.send({
								data: [...mobileResponse2],
								status: 200
							})
						}).catch((error1) => {
							console.log("error ==> 1", error1)
						});

				}

			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});

	//Fetch duplicate serial number
	router.get("/duplicateserialnumber", async (req, res) => {
		let sql1 = `select serial_number,count(serial_number) from public."Assets" where serial_number is not null and serial_number !='' and serial_number not like '--%' and serial_number not like '....%' and UPPER(serial_number) not in( '--%', '....%','XXXXXX','N/A','UNKNOWN') and asset_status ='not_archived' and UPPER(asset_type) in ('COMPUTER','MOBILE','MOBILE DEVICE','MOBILE DEVICES') group by serial_number having count(serial_number) >1`
		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});

	//excel export and send mail

	let excelcolumns = [
		{ header: 'Project ID', key: 'project_id' },
		{ header: 'Asset ID', key: 'asset_id' },
		{ header: 'Client name', key: 'client_name' },
		{ header: 'Asset type', key: 'asset_type' },
		{ header: 'Form factor', key: 'form_factor' },
		{ header: 'Quantity', key: 'quantity' },
		{ header: 'Manufacturer', key: 'manufacturer' },
		{ header: 'Model', key: 'model' },
		{ header: 'IMEI', key: 'imei' },
		{ header: 'Serial Number', key: 'serial_number' },
		{ header: 'processor', key: 'processor' },
		{ header: 'memory', key: 'memory' },
		{ header: 'HDD', key: 'hdd' },
		{ header: 'Grade', key: 'grade' },
		{ header: 'Complaint', key: 'complaint' },
		{ header: 'Complaint_1', key: 'complaint_1' },
	];
	router.post("/excelexportsendmail", async (req, res) => {
		let prjData = (req.body);
		let emails = []
		const filename = `product report - ${prjData.id}.xlsx`;
		let workbook = new Excel.Workbook();
		let worksheet = workbook.addWorksheet(`Product report - ${prjData.id}`);
		worksheet.columns = excelcolumns;
		let prodUsers = `SELECT distinct b.email FROM public.project_product_report_users a, public.directus_users b where a.project_users_id = b.id and project_id = ${prjData.id}`;
		await database.raw(prodUsers)
			.then(async (response) => {
				let result1 = response.rows;
				emails = result1.map(
					(item) => item.email
				);
				// if (result1?.length > 0) {
				let sql3 = `select c.client_name,ast.asset_id,p.id as project_id,asset_type,form_factor,quantity,manufacturer,model,imei,serial_number,processor,memory,hdd,grade,complaint_1,complaint from public.project p INNER JOIN public.clients c ON p.id = ${prjData.id} and c.id = p.client INNER JOIN public."Assets" ast ON ast.project_id = p.id`;
				await database.raw(sql3)
					.then(async (response) => {
						let result = response.rows;
						if (result?.length > 0) {
							let msg = ''
							if (prjData.isSendMail) {
								if (emails?.length > 0) {
									result.forEach((e) => {
										worksheet.addRow(e);
									});
									body = `<table style="font-size: 14px;width:56%"><tr><div style="font-weight: 200;"><span>Hi,</span><br/> Please find the attachement for product report</div></tr></table>`;

									const buffer = await workbook.xlsx.writeBuffer();

									emails.forEach(async (email) => {
										await mailService.send({
											to: email,
											subject: `Product report - ${prjData.id}`,
											html: body,
											attachments: [
												{
													filename,
													content: buffer,
													contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
												},
											],
										}).catch((error) => {
											console.log("mail errrr 111", error)
										});
									})
									msg = 'Mail sent successfully.'

								} else {
									msg = 'No E-mails are linked.';
								}
							} else {
								msg = 'Assets downloaded successfully.';
							}
							res.send({
								data: result,
								status: 200,
								msg: msg
							})

						} else {
							res.send({
								data: [],
								status: 200,
								msg: 'No assets found.'
							})
						}
					})
					.catch((error) => {
						res.send({
							data: [],
							status: 500,
							msg: 'Something went worng'
						})
					});

				// } else {
				// 	res.send({
				// 		data: [],
				// 		status: 200,
				// 		msg: 'Something went worng'
				// 	})
				// }
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});

	});


	//asset value techkonsulter external page
	router.get("/asset_value", async (req, res) => {
		let sql1 = `select UPPER(form_factor) form_factor,UPPER(processor) processor, UPPER(model) model,count(*) sold_qty,sum(sold_price) tot_soldprice from public."Assets" where processor !='' and model !='' and UPPER(form_factor) in ('DESKTOP','LAPTOP','PHONE','TABLET') and UPPER(grade) in ('A','A+','NEW','NOB','AV','AB') and date_nor != 'Invalid date' and date_nor != '' and date_nor is not null and  date_nor::date >  CURRENT_DATE - INTERVAL '60 day'  group by UPPER(form_factor),UPPER(processor),UPPER(model)`

		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});

	// Download pdf
	router.get("/certus_pdf_download", async (req, res) => {
		let asset_id = req.query.asset_id
		let asset_type = req.query.asset_type
		var request = require("request");

		if (!asset_type) {
			res.send({
				data: [],
				status: 500,
				msg: 'No records found.'
			})
		}
		let erasure = '';
		let field = '';

		if (asset_type === 'MOBILE DEVICE') {
			erasure = 'cemd';
			field = 'field2';

		} else if (asset_type === 'COMPUTER') {
			erasure = 'ce'
			field = 'field5';

		}
		let url = `https://cloud.certus.software/webservices/rest-api/v1/reports/${erasure}`;
		request.post({
			url: url,
			headers: {
				"Customer-Code": "220811",
				Authorization: "Basic dG9yZC5oZW5yeXNvbjpJdG9ub215OEA=",
				"content-type": "application/json",
			},
			body: `{
						"reportMode": "ORIGINAL",
						"groupData": "DRIVE",
						"request": {
						  "filter": {
							"criteria": [
								{
									"column": "cewm.${erasure}.report.document.custom.${field}",
									"conditions": [{
										"type": "text",
										"operator": "eq",
										"value": "${asset_id}"
										}
									]
								}
							]
						  }
						},
					  "response": {
							  "showColumns": ["cewm.${erasure}.report.erasure.pdf"]
							}
					  }`,
		},
			async function (error, response) {
				if (error) {
					// res.status("500").send(error);
					console.log("eroororr certus", error);
					res.send({
						data: []
					})
				} else {
					if (response.body && JSON.parse(response.body).length > 0) {
						res.send({
							data: JSON.parse(response.body),
							status: 200,
							msg: 'Pdf downloaded Successfully.'
						})
					}
				}

			}
		);
	});

	// update project finance
	router.get("/updateprojectfinance", async (req, res) => {
		let ids = ''
		console.log("req.query.ids", req.query.ids)
		if (req.query.ids) {
			ids = req.query.ids.split(',')
			if (ids) {
				let result;
				for (let i = 0; i < ids.length; i++) {
					result = await UPDATEPROJECTFINANCE(ids[i], projectService, database, res)
				}
				res.json({
					msg: 'Updated project finance'
				});
			}
		}

	});

	//Fetchh asset details for Mobiel app

	router.get("/fetchAssets", async (req, res) => {
		try {
			if (req.query.asset_id && !isNaN(req.query.asset_id)) {
				const asset_service = await assetsService.readByQuery({
					fields: ["deviations", "asset_id", "asset_type", "imei", "serial_number", "project_id", "status", "complaint_from_app", "grade_from_app", "deviations_from_app", "grade"],
					filter: {
						_and: [
							{
								asset_id: { _eq: req.query.asset_id }
							}
						]
					}
				});
				if (asset_service?.length > 0) {
					res.send({
						data: asset_service,
						status: 200
					})
				} else {
					res.send({
						data: [],
						status: 500
					})
				}
			} else {
				res.send({
					data: [],
					status: 500
				})
			}

		} catch (e) {
			res.send({
				data: [],
				status: 500
			})
			// throw new ServiceUnavailableException(e);
		}
	});
	//Fetchh complaints

	router.get("/complaints", async (req, res) => {
		const complaints_Service = await complaintsService.readByQuery({
			fields: ["id", "complaint_types", "name", "short_name", "asset_types", "sorting"],
			limit: -1,
			sort: ['-count']
		});
		if (complaints_Service?.length > 0) {
			let data = [];
			if (req.query.type) {
				(complaints_Service).forEach((item) => {
					if (item.asset_types && item.asset_types.includes(req.query.type)) {
						item.sorting_count = item.sorting[req.query.type]
						data.push(item)
					}
				})
				data.sort((a, b) => b.sorting_count - a.sorting_count);
			} else {
				data = complaints_Service
			}
			res.send({
				data: data,
				status: 200
			})
		} else {
			res.send({
				data: [],
				status: 500
			})
		}
	});
	// Fetch customer-stocklist

	router.get("/customer-stocklist", async (req, res) => {
		// let sql1_old = `select asset_type as Type,UPPER(manufacturer) as manufacturer,UPPER(model) as model,UPPER(processor) as CPU,"Part_No" as "Part number", MIN(sold_price) as min_sales,MAX(sold_price) as max_sales, count(*) sold_qty, sum(sold_price) tot_soldprice  from public."Assets" where date_nor != '' and date_nor::date >  CURRENT_DATE - INTERVAL '3 months' and UPPER(status) NOT like 'IN STOCK' and UPPER(status) NOT like 'RECYCLED' and (UPPER(status) like 'SOLD' or UPPER(status) like 'RESERVATION') and (grade = 'A' or grade = 'B') group by asset_type,"Part_No",UPPER(manufacturer),UPPER(model),UPPER(processor)`
		//let sql1 = `select  "Part_No" as "Part Number",UPPER(grade) as "Condition",model as "Description",manufacturer as "Manufacturer",target_price as "Price",sum(quantity) as "Quantity"  from public."Assets" where "Part_No" is not null and "Part_No" != '' and (UPPER(asset_type) = 'NETWORK' OR UPPER(asset_type) = 'SERVER & STORAGE' OR UPPER(asset_type) = 'DOCKING') and LOWER(status) = 'in stock' and (UPPER(grade) = 'A' or UPPER(grade) = 'NEW' OR UPPER(grade) = 'NOB') group by UPPER(grade), "Part_No",manufacturer,target_price,model`
		let sql1 = `select asset_type,form_factor,"Part_No",manufacturer,grade, concat(model,' ',processor,' ',memory,' ',hdd,' ', screen) as description, sum(quantity) as quantity from public."Assets"  ast INNER JOIN public.project prj on prj.id = ast.project_id and prj.warehouse = 'SE01' and  ast.status = 'IN STOCK' and asset_type is not null and asset_type !='ADAPTER' and grade is not null and grade in ('A+','A', 'B', 'C', 'NEW', 'NOB') group by asset_type, form_factor, "Part_No", manufacturer, model, processor,memory,hdd,grade,screen,data_destruction order by asset_type, form_factor,"Part_No", manufacturer, description`

		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				result.forEach((obj) => {
					if (obj.hdd !== 'N/A') {
						obj.data_destruction = 'Erased'
					}
					obj.quantity = Number(obj.quantity);

				})
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500
				})
			});
	});

	//Nerdfix user update
	router.post("/updateasset", async (req, res) => {

		let assetsData = (req.body);

		let asset_id_nl = assetsData.asset_id_nl
		delete assetsData.asset_id;
		delete assetsData.target_price;
		delete assetsData.sold_order_nr;
		assetsData.date_created = moment(assetsData.created_at);
		assetsData.created_at = moment(assetsData.created_at);
		let nerdfixTypes = assetsData.asset_type ? assetsData.asset_type.toLowerCase() : null;
		if (nerdfixTypes) {
			if (nerdfixTypes === "all-in one") {
				assetsData.asset_type = 'COMPUTER'
			} else if (nerdfixTypes === "desktop") {
				assetsData.asset_type = 'COMPUTER'
			} else if (nerdfixTypes === "mobile") {
				assetsData.asset_type = 'MOBILE DEVICE'
			} else if (nerdfixTypes === "monitor") {
				assetsData.asset_type = 'MONITOR'
			} else if (nerdfixTypes === "network") {
				assetsData.asset_type = 'NETWORK'
			} else if (nerdfixTypes === "notebook") {
				assetsData.asset_type = 'COMPUTER'
			} else if (nerdfixTypes === "server") {
				assetsData.asset_type = 'SERVER & STORAGE'
			} else if (nerdfixTypes === "tablet") {
				assetsData.asset_type = 'MOBILE DEVICE'
			} else if (nerdfixTypes === "tiny pc") {
				assetsData.asset_type = 'COMPUTER'
			}
		}
		let sql = `select asset_id from public."Assets" where asset_id_nl = '${asset_id_nl}'`
		await database.raw(sql)
			.then(async (results) => {
				let result = results.rows;
				if (result[0]?.asset_id) {
					assetsData.asset_id = result[0].asset_id;
					assetsData.nerdfix_update = 'yes';
					if (!assetsData.form_factor) {
						delete assetsData.form_factor;
					}
					const activity = await assetsService.updateOne(
						result[0].asset_id,
						assetsData
					).then(async (response1) => {
						console.log("update NL product", response1)
						delete assetsData.asset_id;
						res.send({
							data: assetsData,
							status: 200
						})
					}).catch((error1) => {
						console.log("certus mobile update error", error1)
						res.send({
							data: [],
							status: 500,
							message: error1
						})
					});

				} else {
					res.send({
						data: [],
						status: 200,
						message: 'No data found'
					})
				}
			})
			.catch((error) => {
				res.send({
					data: [],
					status: 500,
					message: error
				})
			});


	});

	//Nerdfix transport API
	router.post("/updateNerdfixTransport", async (req, res) => {
		if (req.body && req.body?.products?.length === 0) {
			res.send({
				data: [],
				status: 500,
				message: 'There is no records.'
			})
		}
		let result = await NERDFIXBULKUPDATE(req.body, nerdfixservice);
		if (result) {
			res.send({
				message: 'Assets sucessfully submitted to nerdfix',
				status: 200
			})
		}
	});

	// Update revisions

	router.get("/update_revision", async (req, res) => {
		if (req.query.id) {
			let sql1 = `update public.directus_revisions set is_deleted = true where id =${req.query.id}`
			await database.raw(sql1)
				.then(async (response) => {

					res.send({
						status: 200,
						message: 'Successfully updated.'
					})
				})
				.catch((error) => {
					res.send({
						data: [],
						status: 500
					})
				});
		} else {
			res.send({
				data: [],
				status: 500,
				message: 'ID is mandatory.'
			})
		}

	});

	router.get("/certus", async (req, res) => {
		let sql11 = `select ast.asset_id,ast.graphic_card from public."Assets" ast INNER JOIN public."Certus" ct on ct.asset_id is not null and ct.asset_id !='' and ct.asset_id not like '% %' and ct.asset_id not like '%l%'  and ct.asset_id not like '%L%' and ct.asset_id not like '%e%' and ct.asset_id not like '%z%' and ct.asset_id not like '%-%' and ct.asset_id not like '%½%' and ct.asset_id not like '%TEST%' and ct.asset_id not like '%,%' and ct.asset_id not like '%s%'  and ct.asset_id != '%' and ct.asset_id::int = ast.asset_id::int and char_length(ct.asset_id) <= 6 and ct.asset_id ~ '^[0-9]+$' and ct.asset_id !='3O886' and ast.graphic_card is null and data_generated ='CERTUS'`
		let sql1 = `select asset_id,graphic_card from public."Certus" ct order by ct.asset_id::int desc`
		await database.raw(sql1)
			.then(async (response) => {
				let result = response.rows;
				console.log("resultttt", result)
				res.send({
					data: result,
					status: 200
				})
			})
			.catch((error) => {
				console.log("error", error)
				res.send({
					data: [],
					status: 500
				})
			});


	});

};