#pragma once

#include <unordered_set>
#include <algorithm>
//#include <execution>
#include <ppl.h>
#include "Utils.h"

namespace MTObjects
{
using std::unordered_set;

template<typename T>
//using FastContainer = vector<T, std::pmr::polymorphic_allocator>;
using FastContainer = SmartStack<T>;

struct Cluster;

struct IThreadSafeObject
{
public:
	Cluster* cluster_ = nullptr;

	virtual void IsDependentOn(FastContainer<IThreadSafeObject*>& ref_dependencies) const = 0;
	virtual void IsConstDependentOn(unordered_set<const Cluster*>& ref_dependencies) const = 0;

	virtual void Task()
	{
		cluster_ = nullptr;
	}
};

struct Cluster
{
	FastContainer<IThreadSafeObject*> objects_;			  //no duplicates
	unordered_set<const Cluster*> const_dependencies_clusters_;

	int index_in_clusters_vec_ = -1;

	void Reset()
	{
		objects_.clear();
		const_dependencies_clusters_.clear();
		index_in_clusters_vec_ = -1;
	}

private:

	void MergeTo(Cluster& main_cluster)
	{
		for (auto obj : objects_)
		{
			obj->cluster_ = &main_cluster;
#ifdef TEST_STUFF 
			if (objects_.size() > 1)
			{
				TestStuff::cluster_in_obj_overwritten++;
			}
#endif //TEST_STUFF
		}
		ContainerFunc::Merge(main_cluster.objects_, objects_);
	}

	static void RemoveCluster(vector<Cluster*> &clusters, const Cluster* cluster)
	{
		const int index_to_reuse = cluster->index_in_clusters_vec_;
		if (index_to_reuse != -1)
		{
			const auto last_index = clusters.size() - 1;
			if (last_index != index_to_reuse)
			{
				Cluster* const moved_cluster = clusters[last_index];
				clusters[index_to_reuse] = moved_cluster;
				moved_cluster->index_in_clusters_vec_ = index_to_reuse;
			}
			clusters.pop_back();
		}
	}

	static Cluster* GatherObjects(Cluster* new_cluster, IThreadSafeObject* in_obj, vector<Cluster*>& clusters)
	{
		Cluster* actual_cluster = new_cluster;
		FastContainer<IThreadSafeObject*> objects_to_handle;
		objects_to_handle.push_back(in_obj);
		while (!objects_to_handle.empty())
		{
			IThreadSafeObject* obj = objects_to_handle.back();
			objects_to_handle.pop_back();
			if (nullptr == obj->cluster_)
			{
				actual_cluster->objects_.push_back(obj);
				obj->cluster_ = actual_cluster;
				obj->IsDependentOn(objects_to_handle);
#ifdef TEST_STUFF 
				TestStuff::max_num_objects_to_handle = std::max<size_t>(objects_to_handle.size(), TestStuff::max_num_objects_to_handle);
#endif //TEST_STUFF	
			}
			else if (obj->cluster_ != actual_cluster)
			{
				const bool use_new_cluster = obj->cluster_->objects_.size() > actual_cluster->objects_.size();
				Cluster* to_merge = use_new_cluster ? actual_cluster : obj->cluster_;
				actual_cluster = use_new_cluster ? obj->cluster_ : actual_cluster;

				to_merge->MergeTo(*actual_cluster);
				RemoveCluster(clusters, to_merge);
				to_merge->Reset();
			}
		}
		return actual_cluster;
	}

	static void CreateClusters(const vector<IThreadSafeObject *> &all_objects, vector<Cluster*> &clusters, vector<Cluster>& preallocated_clusters)
	{
		unsigned int cluster_counter = 0;
		for(unsigned int first_remaining_obj_index = 0; first_remaining_obj_index < all_objects.size(); first_remaining_obj_index++)
		{
			if(nullptr != all_objects[first_remaining_obj_index]->cluster_)
			{
				continue;
			}

			Assert(cluster_counter < preallocated_clusters.size());
			Cluster* new_cluster = &preallocated_clusters[cluster_counter];
			Cluster* acltual_cluster = GatherObjects(new_cluster, all_objects[first_remaining_obj_index], clusters);
			if (acltual_cluster == new_cluster)
			{
				new_cluster->index_in_clusters_vec_ = static_cast<int>(clusters.size());
				clusters.emplace_back(new_cluster);
				cluster_counter++;
#ifdef TEST_STUFF 
				TestStuff::max_num_clusters = std::max<std::size_t>(TestStuff::max_num_clusters, clusters.size());
#endif //TEST_STUFF
			}
		}
	}

	static void CreateClustersDependencies(const vector<Cluster*>& clusters)
	{
		auto num_clusters = clusters.size();
		//std::for_each(std::execution::par_unseq,
		concurrency::parallel_for_each(
			std::begin(clusters), std::end(clusters),
		[num_clusters](Cluster* cluster){
			cluster->const_dependencies_clusters_.clear();
			//cluster->const_dependencies_clusters_.reserve(num_clusters);
			for (auto obj : cluster->objects_)
			{
				Assert(obj->cluster_ == cluster);
				obj->IsConstDependentOn(cluster->const_dependencies_clusters_);
			}
			cluster->const_dependencies_clusters_.erase(cluster);
		});
	}

public:
	static vector<Cluster*> GenerateClusters(const vector<IThreadSafeObject*>& all_objects, vector<Cluster>& preallocated_clusters)
	{
		vector<Cluster*> clusters;
		clusters.reserve(preallocated_clusters.size());
		CreateClusters(all_objects, clusters, preallocated_clusters);
		CreateClustersDependencies(clusters);
		return clusters;
	}
#ifdef TEST_STUFF 
	static bool Test_AreClustersCoherent(const vector<Cluster*>& clusters)
	{
		// All dependencies of the objects must be inside the cluster
		for (auto cluster : clusters)
		{
			auto& objects = cluster->objects_;
			for (auto obj : objects)
			{
				Assert(obj && obj->cluster_ == cluster);

				FastContainer<IThreadSafeObject*> dependencies;
				obj->IsDependentOn(dependencies);
				for (auto dep : dependencies)
				{
					Assert(dep && dep->cluster_ == cluster);
				}
			}
			vector<IThreadSafeObject*> objects_vec(objects.begin(), objects.end());
			std::sort(objects_vec.begin(), objects_vec.end());
			for (auto iter = objects_vec.begin();;)
			{
				auto obj = *iter;
				iter++;
				if (iter == objects_vec.end())
					break;
				Assert(obj != *iter);
			}
		}
		return true;
	}
#endif //TEST_STUFF
};

struct GroupOfConcurrentClusters
{
	vector<Cluster*> clusters_;
	unordered_set<const Cluster*> const_dependencies_clusters_;

	void AddCluster(Cluster& cluster)
	{
		clusters_.emplace_back(&cluster);
		const_dependencies_clusters_.insert(cluster.const_dependencies_clusters_.begin(), cluster.const_dependencies_clusters_.end());
	}

	static vector<GroupOfConcurrentClusters> GenerateClusterGroups(const vector<Cluster*>& clusters)
	{
		auto depends_on = [](const Cluster* a, const Cluster* b) -> bool
		{
			return a->const_dependencies_clusters_.end() != a->const_dependencies_clusters_.find(b);
		};

		auto fits_into_group = [&](const GroupOfConcurrentClusters& group, const Cluster* cluster) -> bool
		{
			return std::none_of(group.clusters_.begin(), group.clusters_.end(), [&](const Cluster* cluster_in_group)
			{
				return depends_on(cluster, cluster_in_group);
			}) 

			&& group.const_dependencies_clusters_.find(cluster) == group.const_dependencies_clusters_.end();
		};

		vector<GroupOfConcurrentClusters> groups;
		groups.reserve(clusters.size()/4);
		groups.resize(1); //groups.resize(std::max<size_t>(1, clusters.size() / 32));
		size_t cluster_index = 0;
		for (auto cluster : clusters)
		{
			bool fits_in_existing_group = false;
			{
				const size_t group_num = groups.size();
				const size_t first_group_to_try = cluster_index % group_num;
				for (size_t groups_checked = 0; groups_checked < group_num && !fits_in_existing_group; groups_checked++)
				{
					const size_t group_idx = (first_group_to_try + groups_checked) % group_num;
					auto& group = groups[group_idx];
					if (fits_into_group(group, cluster))
					{
						group.AddCluster(*cluster);
						fits_in_existing_group = true;
					}
				}
			}
			if (!fits_in_existing_group)
			{
				GroupOfConcurrentClusters new_group;
				new_group.AddCluster(*cluster);
				groups.emplace_back(new_group);
			}
			cluster_index++;
		}
		return groups;
	}
};
}
